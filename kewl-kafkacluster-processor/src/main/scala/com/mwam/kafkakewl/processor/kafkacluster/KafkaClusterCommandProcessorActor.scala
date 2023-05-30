/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster

import akka.actor.Actor
import akka.util.Timeout
import com.mwam.kafkakewl.common.AllDeploymentEntities.InMemoryStateStores._
import com.mwam.kafkakewl.common.AllDeploymentEntities.WritableStateStores._
import com.mwam.kafkakewl.common._
import com.mwam.kafkakewl.common.persistence.PersistentStoreFactory
import com.mwam.kafkakewl.common.validation.validateDeploymentStateStore
import com.mwam.kafkakewl.domain.Command.{HealthIsLive, HealthIsLiveResponse, HealthIsReady, HealthIsReadyResponse}
import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.domain.deploy.{DeployedTopology, DeployedTopologyStateChange}
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.kafka.utils.KafkaConnection
import com.mwam.kafkakewl.utils.{ApplicationMetrics, MdcUtils}
import com.typesafe.scalalogging.LazyLogging

object KafkaClusterCommandProcessorActor {
  final case class Config(
    kafkaClusterId: KafkaClusterEntityId,
    env: Env,
    connection: KafkaConnection,
    kafkaCluster: KafkaCluster,
    topicDefaults: TopicDefaults,
    failFastIfDeploymentStateStoreInvalid: Boolean
  )

  final case class Result(
    commandResult: KafkaClusterCommandResult,
    changes: AllDeploymentEntitiesStateChanges = AllDeploymentEntitiesStateChanges()
  )

  class RestartMe(kafkaClusterId: KafkaClusterEntityId, reason: String) extends RuntimeException(s"$kafkaClusterId: requiring restart because of $reason") {}

  sealed trait OutgoingMessage
  final case class DeployedTopologies(kafkaClusterId: KafkaClusterEntityId, deployedTopologies: IndexedSeq[EntityState.Live[DeployedTopology]]) extends OutgoingMessage
  final case class DeployedTopologyChanges(kafkaClusterId: KafkaClusterEntityId, changes: IndexedSeq[DeployedTopologyStateChange.StateChange]) extends OutgoingMessage
  final case class GetKafkaCluster(kafkaClusterId: KafkaClusterEntityId) extends OutgoingMessage

  final case class UpdateKafkaCluster(kafkaCluster: KafkaCluster)
  final case class ReInitialize(wipe: Boolean)
}

class KafkaClusterCommandProcessorActor(
  config: KafkaClusterCommandProcessorActor.Config,
  persistentStoreFactory: PersistentStoreFactory
)(implicit timeout: Timeout) extends Actor with ActorPreRestartLog with LazyLogging with MdcUtils {
  // not using akka's ActorLogging because that's async and everywhere else we use the normal LazyLogging (performance won't matter anyway)
  import context.dispatcher

  override val actorMdc: Map[String, String] = Mdc.fromKafkaClusterId(config.kafkaClusterId)

  private[this] var kafkaCluster: KafkaCluster = config.kafkaCluster
  private[this] var persistentStore: AllDeploymentEntitiesPersistentStore = _

  private[this] var inMemoryStateStores: AllDeploymentEntities.InMemoryStateStores = _
  private[this] var kafkaClusterAdmin: DefaultKafkaClusterAdmin = _
  private[this] var kafkaClusterCommandProcessor: KafkaClusterCommandProcessing = _

  private[this] def processCommand(
    command: KafkaClusterCommand,
    stateStores: AllDeploymentEntities.ReadableStateStores
  ): KafkaClusterCommandProcessorActor.Result = {
    kafkaClusterCommandProcessor.process(kafkaCluster, command, stateStores.deployedTopology)
      // converting the either-result to a proper result of this command processing
      .map(r => KafkaClusterCommandProcessorActor.Result(r.commandResult, r.entityChanges))
      .left.map(f => KafkaClusterCommandProcessorActor.Result(f))
      .merge
  }

  override def preStart(): Unit = {
    logger.info(s"preStart() starting...")

    super.preStart()

    persistentStore = persistentStoreFactory.createForDeploymentEntities(config.kafkaClusterId, config.kafkaCluster, config.connection, logger, startWithWipe = false)

    inMemoryStateStores = persistentStore.loadLatestVersions()

    logger.info(s"loaded ${inMemoryStateStores.deployedTopology.getLatestLiveStates.size} deployedTopologies")

    if (config.failFastIfDeploymentStateStoreInvalid) {
      validateDeploymentStateStore(logger, inMemoryStateStores, kafkaCluster, config.topicDefaults)
    } else {
      logger.info("skipping validating the deployment state store")
    }

    kafkaClusterAdmin = new DefaultKafkaClusterAdmin(config.kafkaClusterId, config.connection, config.kafkaCluster)
    kafkaClusterCommandProcessor = new KafkaClusterCommandProcessing(config.kafkaClusterId, config.connection, kafkaClusterAdmin, config.topicDefaults)

    context.parent ! KafkaClusterCommandProcessorActor.DeployedTopologies(config.kafkaClusterId, inMemoryStateStores.deployedTopology.getLatestLiveStates)
    // TODO the response to this may arrive later than some other message, in which case those messages will use an out-of-date kafkaCluster.nonKewl
    context.parent ! KafkaClusterCommandProcessorActor.GetKafkaCluster(config.kafkaClusterId)

    logger.info(s"preStart() finished.")
  }

  override def aroundReceive(receive: Actor.Receive, msg: Any): Unit = {
    msg match {
      case commandBase: CommandBase =>
        withMDC(commandBase.mdc) {
          super.aroundReceive(receive, msg)
        }
      case _ =>
        super.aroundReceive(receive, msg)
    }
  }

  override def receive: Receive = {
    case command: HealthIsLive =>
      logger.info(s"received: $command")
      // it's live when it received this request
      sender() ! HealthIsLiveResponse(live = true, Seq.empty)

    case command: HealthIsReady =>
      logger.info(s"received: $command")
      // it's ready when it received this request (preStart sorted out all start-up operations)
      sender() ! HealthIsReadyResponse(ready = true, Seq.empty)

    case command: KafkaClusterCommand =>
      logger.info(s"received: $command")
      val result = processCommand(command, inMemoryStateStores.toReadable)
      if (result.changes.nonEmpty && !command.dryRun) {
        // persist the state changes in memory...
        inMemoryStateStores.toWritable.applyEntityStateChange(result.changes)
        // ...and in the persistent store too
        persistentStore.saveTransactions(result.changes.toTransactionItems)
        // notifying the parent
        if (result.changes.deployedTopology.nonEmpty) {
          context.parent ! KafkaClusterCommandProcessorActor.DeployedTopologyChanges(config.kafkaClusterId, result.changes.deployedTopology)
        }
      }
      sender() ! result

    case updateKafkaCluster: KafkaClusterCommandProcessorActor.UpdateKafkaCluster =>
      logger.info(s"received: $updateKafkaCluster")
      kafkaCluster = updateKafkaCluster.kafkaCluster

    case command: KafkaClusterCommandProcessorActor.ReInitialize =>
      logger.warn(s"received: $command")
      if (command.wipe) {
        persistentStore.wipe()
      }
      // either way trigger a restart with an exception, we'll reload our state
      throw new KafkaClusterCommandProcessorActor.RestartMe(config.kafkaClusterId, s"reinitialization(wipe=${command.wipe})")

    case msg =>
      ApplicationMetrics.errorCounter.inc()
      logger.error(s"received unknown message: $msg")
  }

  override def postStop(): Unit = {
    logger.info(s"postStop() starting...")
    super.postStop()
    kafkaClusterAdmin.close()
    persistentStore.close()
    logger.info(s"postStop() finished.")
  }
}
