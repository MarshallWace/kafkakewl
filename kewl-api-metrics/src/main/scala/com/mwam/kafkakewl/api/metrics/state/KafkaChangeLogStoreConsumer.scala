/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.state

import java.util.concurrent.atomic.AtomicBoolean

import com.mwam.kafkakewl.kafka.utils.{KafkaConnection, KafkaConsumerConfig}
import com.mwam.kafkakewl.common.{AllDeploymentEntities, AllStateEntities, ApplyStateChange}
import com.mwam.kafkakewl.common.AllStateEntities.InMemoryStateStores._
import com.mwam.kafkakewl.common.AllStateEntities.WritableStateStores._
import com.mwam.kafkakewl.common.AllDeploymentEntities.InMemoryStateStores._
import com.mwam.kafkakewl.common.AllDeploymentEntities.WritableStateStores._
import com.mwam.kafkakewl.common.changelog.{KafkaChangeLogStore, ReceivedChangeLogMessage}
import com.mwam.kafkakewl.domain.EntityState
import com.mwam.kafkakewl.domain.JsonEncodeDecode._
import com.mwam.kafkakewl.domain.deploy.{DeployedTopology, DeployedTopologyEntityId}
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent._
import com.mwam.kafkakewl.utils._

import scala.collection.mutable
import scala.concurrent.duration.Duration

object KafkaChangeLogStoreConsumer {
  final case class KafkaClusterObserverFunctions(
    kafkaClusterCreated: (KafkaClusterEntityId, KafkaCluster) => Unit,
    kafkaClusterUpdated: (KafkaClusterEntityId, KafkaCluster, KafkaCluster) => Unit,
    kafkaClusterDeleted: (KafkaClusterEntityId, KafkaCluster) => Unit
  )

  final case class DeployedTopologyObserverFunctions(
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
      logger.info(s"$partitionOffsetString: $transaction")
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
            logger.info(s"$partitionOffsetString: KafkaCluster#${deletedKafkaCluster.id} is deleted, removing its deployment store")
            deploymentStores.remove(KafkaClusterEntityId(deletedKafkaCluster.id))

          case ApplyStateChange.ApplyResult(None) => () // no change applied at all, nothing to do
        }
    }

    receivedChangeLogMessage.message.deployment.foreach { transaction =>
      logger.info(s"$partitionOffsetString: $transaction")
      receivedChangeLogMessage.message.kafkaClusterId.foreach { kafkaClusterId =>
        // making sure that we have deployment store for this kafka-cluster
        val deploymentStateStore = deploymentStores.getOrElse(kafkaClusterId, {
          val newDeploymentStateStore = AllDeploymentEntities.InMemoryStateStores().toWritable
          logger.info(s"$partitionOffsetString: KafkaCluster#${kafkaClusterId.id} is needed, add its deployment store")
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

    // the order of notifications matters here unfortunately (it's bad, but has reasons):
    // basically we want the deployed topology stuff to get to its recipients (ConsumerGroupStatusEvaluatorOfKafkaCluster) so that it what it wants to know BEFORE
    // the kafka-cluster metrics collector starts up.
    // The proper solution would be to make everything actors including the ConsumerGroupStatusEvaluatorOfKafkaCluster and ConsumerGroupStatusEvaluators and then we wouldn't have to
    // worry about race conditions between notifications from here and notifications from the kafka metrics collectors

    // first notifying about deployedTopologies
    deployedTopologies.foreach { case (id, deployedTopology) => deployedTopologyObserver.deployedTopologyCreated(id, deployedTopology) }

    // then about kafkaClusters
    kafkaClusters.foreach { case (id, kafkaCluster) => kafkaClusterObserver.kafkaClusterCreated(id, kafkaCluster) }
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
