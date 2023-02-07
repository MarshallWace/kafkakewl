/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.state

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.mwam.kafkakewl.common.AllStateEntities.InMemoryVersionedStateStores._
import com.mwam.kafkakewl.common.AllStateEntities.ReadableVersionedStateStores._
import com.mwam.kafkakewl.common.AllStateEntities.WritableStateStores._
import com.mwam.kafkakewl.common._
import com.mwam.kafkakewl.common.cache.{AllStateEntitiesStateStoresCache, DeployedTopologyStateStoresCache}
import com.mwam.kafkakewl.common.metrics.MetricsServiceOps
import com.mwam.kafkakewl.common.persistence.PersistentStoreFactory
import com.mwam.kafkakewl.common.validation.{PermissionValidator, validateStateStore}
import com.mwam.kafkakewl.domain.AllStateEntitiesStateChanges._
import com.mwam.kafkakewl.domain.deploy.DeployedTopology
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.{Command, _}
import com.mwam.kafkakewl.processor.state.StateCommandProcessingValidator.ValidatorFunc
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContextExecutor

object StateCommandProcessor {
  final case class Config(
    env: Env,
    kafkaClusterCommandProcessorJaasConfig: Option[String], // the only purpose of this here is validation
    metricsService: Option[MetricsServiceOps],
    failFastIfStateStoreInvalid: Boolean
  )

  final case class Result(
    commandResult: CommandResult.Succeeded,
    changes: AllStateEntities.ApplyEntityStateChangeResults = AllStateEntities.ApplyEntityStateChangeResults(),
    kafkaClusterCommands: Seq[KafkaClusterCommand] = Seq.empty
  )
}

/**
  * This class is responsible for processing state-commands, maintaining the current set of state.
  *
  * It's used only from the main command processor actor, which means we don't need to care about multi-threading.
  *
  * @param config the configuration
  * @param permissionValidator permission validator
  * @param persistentStoreFactory the persistent store factory to create a persistent store to load the entities
  * @param stateStoresCache the state cache which gets updated whenever the state changes
  * @param deployedTopologyStateStoresCache the state cache which gets updated whenever the deployed topology state changes
  * @param startWithWipe if true, it wipes the persistent store and starts with an empty state
  * @param topicDefaults the topic defaults
  */
class StateCommandProcessor(
  config: StateCommandProcessor.Config,
  permissionValidator: PermissionValidator,
  persistentStoreFactory: PersistentStoreFactory,
  stateStoresCache: AllStateEntitiesStateStoresCache,
  deployedTopologyStateStoresCache: DeployedTopologyStateStoresCache,
  startWithWipe: Boolean,
  val topicDefaults: TopicDefaults
)(implicit system: ActorSystem, ec: ExecutionContextExecutor, mat: Materializer) extends LazyLogging with StateCommandProcessorCommon with PermissionCommandResultCommon {

  private val validateCommandFunc: ValidatorFunc = StateCommandProcessingValidator.validateCommand(config.kafkaClusterCommandProcessorJaasConfig, topicDefaults)
  private val persistentStore: AllStateEntitiesPersistentStore =
    persistentStoreFactory.createForStateEntities(logger, startWithWipe)
  private val inMemoryStateStores: AllStateEntities.InMemoryVersionedStateStores = {
    val inMemoryStateStores = persistentStore.loadAllVersions()
    logger.info(s"loaded ${inMemoryStateStores.permission.getLatestLiveStates.size} permissions")
    logger.info(s"loaded ${inMemoryStateStores.topology.getLatestLiveStates.size} topologies")
    logger.info(s"loaded ${inMemoryStateStores.kafkaCluster.getLatestLiveStates.size} kafkaClusters")
    logger.info(s"loaded ${inMemoryStateStores.deployment.getLatestLiveStates.size} deployments")

    if (config.failFastIfStateStoreInvalid) {
      validateStateStore(logger, inMemoryStateStores, topicDefaults)
    } else {
      logger.info("skipping validating the state store")
    }

    inMemoryStateStores
  }

  // mutable, gets updated from outside
  private var deployedTopologyStateStores = Map.empty[KafkaClusterEntityId, ReadableStateStore[DeployedTopology]]

  // once the inMemoryStateStores is populated with the initial state, we update the cache too
  updateStateCacheStateStores()

  private def updateStateCacheStateStores(): Unit = stateStoresCache.update(stateStores.snapshot)
  private def updateStateCacheDeployedTopologyStateStores(): Unit = deployedTopologyStateStoresCache.update(deployedTopologyStateStores)

  private def applyPermissionsToCommandResult(
    command: Command,
    result: StateCommandProcessor.Result
  ): Either[CommandResult.Failed, StateCommandProcessor.Result] =
    applyPermissionsToCommandResult(
      permissionValidator,
      stateStores
    )(
      command,
      result.commandResult
    ).map(r => result.copy(commandResult = r))

  def updateDeployedTopologyStateStores(stateStores: Map[KafkaClusterEntityId, ReadableStateStore[DeployedTopology]]): Unit = {
    deployedTopologyStateStores = stateStores
    updateStateCacheDeployedTopologyStateStores()
  }

  def processCommand(
    command: Command.StateCommand
  ): Either[CommandResult.Failed, StateCommandProcessor.Result] = {
    for {
      processingResult <- processCommandWithStateStores(validateCommandFunc, config.metricsService)(command, permissionValidator, inMemoryStateStores.toReadableVersioned, deployedTopologyStateStores)
      commandResult <- {
        val commandResult = processingResult.commandResult
        val entityStateChanges = processingResult.changesAndCommands.entityChanges
        val kafkaClusterCommands = processingResult.changesAndCommands.kafkaClusterCommands
        val entityStateChangeResults =
          if (entityStateChanges.nonEmpty && !command.dryRun) {
            // first persist the changes in kafka (so that if it fails, we don't update the in-memory state at all and we'll be consistent)
            persistentStore.saveTransactions(entityStateChanges.toTransactionItems)
            // persist the state changes in memory too
            val applyEntityStateChangeResults = inMemoryStateStores.toWritable.applyEntityStateChange(entityStateChanges)

            updateStateCacheStateStores()

            applyEntityStateChangeResults
          } else {
            // no need to update the state cache because we haven't applied any changes
            AllStateEntities.ApplyEntityStateChangeResults()
          }

        val unfilteredStateCommandResult = StateCommandProcessor.Result(commandResult, entityStateChangeResults, kafkaClusterCommands)

        applyPermissionsToCommandResult(command, unfilteredStateCommandResult)
      }
    } yield commandResult
  }

  def stateStores: AllStateEntities.ReadableVersionedStateStores = inMemoryStateStores.toReadableVersioned
  def close(): Unit = persistentStore.close()
}
