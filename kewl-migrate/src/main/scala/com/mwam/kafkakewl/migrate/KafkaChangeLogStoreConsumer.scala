/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.migrate

import com.mwam.kafkakewl.common.AllDeploymentEntities.InMemoryStateStores._
import com.mwam.kafkakewl.common.AllDeploymentEntities.WritableStateStores._
import com.mwam.kafkakewl.common.AllStateEntities.InMemoryStateStores._
import com.mwam.kafkakewl.common.AllStateEntities.WritableStateStores._
import com.mwam.kafkakewl.common.changelog.{KafkaChangeLogStore, ReceivedChangeLogMessage}
import com.mwam.kafkakewl.common.{AllDeploymentEntities, AllStateEntities, ApplyStateChange}
import com.mwam.kafkakewl.domain.EntityState
import com.mwam.kafkakewl.domain.JsonEncodeDecode._
import com.mwam.kafkakewl.domain.deploy.{DeployedTopology, DeployedTopologyEntityId}
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.kafka.utils.{KafkaConnection, KafkaConsumerConfig}
import com.mwam.kafkakewl.utils._
import com.typesafe.scalalogging.LazyLogging

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration.Duration

object KafkaChangeLogStoreConsumer {
  final case class KafkaClusterObserverFunctions(
    kafkaClusterInitialSnapshot: Map[KafkaClusterEntityId, KafkaCluster] => Unit,
    kafkaClusterCreated: (KafkaClusterEntityId, KafkaCluster) => Unit,
    kafkaClusterUpdated: (KafkaClusterEntityId, KafkaCluster, KafkaCluster) => Unit,
    kafkaClusterDeleted: (KafkaClusterEntityId, KafkaCluster) => Unit
  )

  final case class DeployedTopologyObserverFunctions(
    deployedTopologyInitialSnapshot: Map[DeployedTopologyEntityId, DeployedTopology] => Unit,
    deployedTopologyCreated: (DeployedTopologyEntityId, DeployedTopology) => Unit,
    deployedTopologyUpdated: (DeployedTopologyEntityId, DeployedTopology, DeployedTopology) => Unit,
    deployedTopologyDeleted: (DeployedTopologyEntityId, DeployedTopology) => Unit
  )
}

class KafkaChangeLogStoreConsumer(
  changeLogKafkaConnection: KafkaConnection,
  kafkaClusterObserver: KafkaChangeLogStoreConsumer.KafkaClusterObserverFunctions,
  deployedTopologyObserver: KafkaChangeLogStoreConsumer.DeployedTopologyObserverFunctions
)(implicit ec: ExecutionContext) extends LazyLogging {
  protected val stopConsuming = new AtomicBoolean(false)

  private val stateStore = AllStateEntities.InMemoryStateStores().toWritable
  private val kafkaClusters = mutable.Map.empty[KafkaClusterEntityId, KafkaCluster]
  private val deployedTopologies = mutable.Map.empty[DeployedTopologyEntityId, DeployedTopology]
  private val deploymentStores = mutable.Map.empty[KafkaClusterEntityId, AllDeploymentEntities.WritableStateStores]

  logger.info("starting consuming")
  private val consumeFuture = Future { blocking { consume() } }
  consumeFuture.crashIfFailed()
  logger.info("consumer running")

  private def handleChangeLogMessage(receivedChangeLogMessage: ReceivedChangeLogMessage, alreadyProcessed: Boolean): Unit = {
    val partitionOffsetString = receivedChangeLogMessage.partitionOffsetString + " " + (if (alreadyProcessed) "reprocessing" else "new")

    receivedChangeLogMessage.message.state.foreach { transaction =>
      val applyResults = stateStore.applyEntityStateChange(transaction.stateChanges)

      // if we got any KafkaCluster CREATE/UPDATE/DELETE state-change => notify the observer
      applyResults.kafkaCluster
        .foreach {
          case ApplyStateChange.ApplyResult(Some(liveKafkaCluster: EntityState.Live[KafkaCluster])) =>
            // notifying the observer (create or update)
            val kafkaClusterId = KafkaClusterEntityId(liveKafkaCluster.id)
            if (!alreadyProcessed) {
              // notifying the observer ONLY when we're not re-processing
              kafkaClusters.get(kafkaClusterId) match {
                case Some(oldKafkaCluster) => kafkaClusterObserver.kafkaClusterUpdated(kafkaClusterId, oldKafkaCluster, liveKafkaCluster.entity)
                case None => kafkaClusterObserver.kafkaClusterCreated(kafkaClusterId, liveKafkaCluster.entity)
              }
            }
            kafkaClusters += (kafkaClusterId -> liveKafkaCluster.entity)

          case ApplyStateChange.ApplyResult(Some(deletedKafkaCluster: EntityState.Deleted[KafkaCluster])) =>
            // notifying the observer (delete, if we have this kafka-cluster at all)
            val kafkaClusterId = KafkaClusterEntityId(deletedKafkaCluster.id)
            if (!alreadyProcessed) {
              // notifying the observer ONLY when we're not re-processing
              kafkaClusters.get(kafkaClusterId).foreach { kafkaCluster => kafkaClusterObserver.kafkaClusterDeleted(kafkaClusterId, kafkaCluster) }
            }
            kafkaClusters.remove(kafkaClusterId)

            // also removing the deployment state-store for this cluster
            deploymentStores.remove(KafkaClusterEntityId(deletedKafkaCluster.id))

          case ApplyStateChange.ApplyResult(None) => () // no change applied at all, nothing to do
        }
    }

    receivedChangeLogMessage.message.deployment.foreach { transaction =>
      receivedChangeLogMessage.message.kafkaClusterId.foreach { kafkaClusterId =>
        // making sure that we have deployment store for this kafka-cluster
        val deploymentStateStore = deploymentStores.getOrElse(kafkaClusterId, {
          val newDeploymentStateStore = AllDeploymentEntities.InMemoryStateStores().toWritable
          deploymentStores += (kafkaClusterId -> newDeploymentStateStore)
          newDeploymentStateStore
        })

        val applyResults = deploymentStateStore.applyEntityStateChange(transaction.stateChanges)

        // if we got any DeployedTopology CREATE/UPDATE/DELETE state-change => notify the observer
        applyResults.deployedTopology
          .foreach {
            case ApplyStateChange.ApplyResult(Some(liveDeployedTopology: EntityState.Live[DeployedTopology])) =>
              // notifying the observer (create or update)
              val deployedTopologyId = DeployedTopologyEntityId(liveDeployedTopology.id)
              if (!alreadyProcessed) {
                // notifying the observer ONLY when we're not re-processing
                deployedTopologies.get(deployedTopologyId) match {
                  case Some(oldDeployedTopology) => deployedTopologyObserver.deployedTopologyUpdated(deployedTopologyId, oldDeployedTopology, liveDeployedTopology.entity)
                  case None => deployedTopologyObserver.deployedTopologyCreated(deployedTopologyId, liveDeployedTopology.entity)
                }
              }
              deployedTopologies += (deployedTopologyId -> liveDeployedTopology.entity)

            case ApplyStateChange.ApplyResult(Some(deletedDeployedTopology: EntityState.Deleted[DeployedTopology])) =>
              // notifying the observer (delete, if we have this deployed topology at all)
              val deployedTopologyId = DeployedTopologyEntityId(deletedDeployedTopology.id)
              if (!alreadyProcessed) {
                // notifying the observer ONLY when we're not re-processing
                deployedTopologies.get(deployedTopologyId).foreach { deployedTopology => deployedTopologyObserver.deployedTopologyDeleted(deployedTopologyId, deployedTopology) }
              }
              deployedTopologies.remove(deployedTopologyId)

            case ApplyStateChange.ApplyResult(None) => () // no change applied at all, nothing to do
          }
      }
    }
  }

  private def handleReprocessCompleted(): Unit = {
    // During re-processing we didn't call any observers (to avoid handling lots of state changes, they need the current state anyway).
    // This is the time for it.
    kafkaClusterObserver.kafkaClusterInitialSnapshot(kafkaClusters.toMap)
    deployedTopologyObserver.deployedTopologyInitialSnapshot(deployedTopologies.toMap)
  }

  private def consume(): Unit = {
    KafkaChangeLogStore.consumeFromBeginningWithoutCommit(
      KafkaConsumerConfig(changeLogKafkaConnection, groupId = None),
      handleChangeLogMessage,
      () => handleReprocessCompleted(),
      stopConsuming,
      logger
    )
  }

  def stop(): Unit = {
    logger.info("stopping consuming")
    stopConsuming.set(true)
    Await.ready(consumeFuture, Duration.Inf)
    logger.info("stopping stopped")
  }
}
