/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.state

import com.mwam.kafkakewl.common._
import com.mwam.kafkakewl.common.validation.PermissionValidator
import com.mwam.kafkakewl.domain.{Command, CommandResult}
import com.mwam.kafkakewl.domain.deploy.DeployedTopology
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.common.AllStateEntities.InMemoryVersionedStateStores._
import com.typesafe.scalalogging.LazyLogging

import java.util.concurrent.atomic.AtomicReference
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.mwam.kafkakewl.common.cache.{AllStateEntitiesStateStoresCache, DeployedTopologyStateStoresCache}
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.processor.state.StateCommandProcessingValidator.ValidatorFunc
import com.mwam.kafkakewl.utils.MdcUtils

import scala.concurrent.ExecutionContextExecutor

class StateReadOnlyCommandProcessor(
  config: StateCommandProcessor.Config,
  permissionValidator: PermissionValidator,
  val topicDefaults: TopicDefaults
)(implicit system: ActorSystem, ec: ExecutionContextExecutor, mat: Materializer) extends LazyLogging
  with AllStateEntitiesStateStoresCache
  with DeployedTopologyStateStoresCache
  with StateCommandProcessorCommon
  with PermissionCommandResultCommon
  with MdcUtils {

  val validateCommandFunc: ValidatorFunc = StateCommandProcessingValidator.validateCommand(config.kafkaClusterCommandProcessorJaasConfig, topicDefaults)
  // this is get/set from multiple threads (in theory a volatile would be enough though)
  val readableStateStores = new AtomicReference[AllStateEntities.ReadableVersionedStateStores](AllStateEntities.InMemoryVersionedStateStores().toReadableVersioned)
  val deployedTopologyStateStores = new AtomicReference(Map.empty[KafkaClusterEntityId, ReadableStateStore[DeployedTopology]])

  def update(stateStores: AllStateEntities.ReadableVersionedStateStores): Unit =
    readableStateStores.set(stateStores)

  def update(stateStores: Map[KafkaClusterEntityId, ReadableStateStore[DeployedTopology]]): Unit =
    deployedTopologyStateStores.set(stateStores)

  def processCommand(command: Command.StateReadOnlyCommand): CommandResult = {
    withMDC(command.mdc) {
      logger.info(s"received: $command")

      val stateStoresSnapshot = readableStateStores.get
      val deployedTopologyStateStoresSnapshot = deployedTopologyStateStores.get

      val commandResultEither = for {
        processingResult <- processCommandWithStateStores(validateCommandFunc, config.metricsService)(command, permissionValidator, stateStoresSnapshot, deployedTopologyStateStoresSnapshot)
        commandResult <- {
          val entityStateChanges = processingResult.changesAndCommands.entityChanges
          val kafkaClusterCommands = processingResult.changesAndCommands.kafkaClusterCommands
          // not expecting state changes or kafka-cluster commands
          assert(entityStateChanges.isEmpty)
          assert(kafkaClusterCommands.isEmpty)

          val unfilteredStateCommandResult = StateCommandProcessor.Result(processingResult.commandResult)

          applyPermissionsToCommandResult(
            permissionValidator,
            stateStoresSnapshot
          )(
            command,
            unfilteredStateCommandResult.commandResult
          )
        }
      } yield commandResult

      val commandResult = commandResultEither.merge
      logger.info(s"response: ${commandResult.truncatedToString}")
      commandResult
    }
  }
}
