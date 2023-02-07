/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.stateadaptor.destinations

import scala.reflect.runtime.universe._
import com.mwam.kafkakewl.kafka.utils.{KafkaConfigProperties, KafkaConnection, KafkaProducerConfig}
import com.mwam.kafkakewl.common.{AllDeploymentEntities, AllStateEntities, ApplyStateChange}
import com.mwam.kafkakewl.common.AllStateEntities.InMemoryStateStores._
import com.mwam.kafkakewl.common.AllStateEntities.WritableStateStores._
import com.mwam.kafkakewl.common.AllDeploymentEntities.InMemoryStateStores._
import com.mwam.kafkakewl.common.AllDeploymentEntities.WritableStateStores._
import com.mwam.kafkakewl.common.changelog.ReceivedChangeLogMessage
import com.mwam.kafkakewl.domain.{Entity, EntityState}
import com.mwam.kafkakewl.domain.JsonEncodeDecode._
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.stateadaptor.StateAdaptorStateful
import io.circe.Encoder
import org.apache.kafka.clients.producer.KafkaProducer

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContextExecutor

object LatestGlobalStateAdaptor {
  final case class Config(destinationKafkaConnection: KafkaConnection, destinationTopicName: String)
}

class LatestGlobalStateAdaptor(
  changeLogKafkaConnection: KafkaConnection,
  changeLogKafkaConsumerGroup: String,
  config: LatestGlobalStateAdaptor.Config,
  timeoutDuration: Duration
)(implicit ec: ExecutionContextExecutor) extends StateAdaptorStateful(changeLogKafkaConnection, changeLogKafkaConsumerGroup) {

  private val stateStore = AllStateEntities.InMemoryStateStores().toWritable
  private val deploymentStores = mutable.Map.empty[KafkaClusterEntityId, AllDeploymentEntities.WritableStateStores]

  private val producer = new KafkaProducer[String, String](KafkaConfigProperties.forProducer(KafkaProducerConfig(config.destinationKafkaConnection, idempotent = Some(true))))

  override protected def handleChangeLogMessage(receivedChangeLogMessage: ReceivedChangeLogMessage, alreadyProcessed: Boolean): Unit = {
    val partitionOffsetString = receivedChangeLogMessage.partitionOffsetString + " " + (if (alreadyProcessed) "reprocessing" else "new")
    def produceStateFunc[E <: Entity : TypeTag](id: String, state: EntityState.Live[E])(implicit encoder: Encoder[EntityState.Live[E]]): Unit = {
      produceState(producer, config.destinationTopicName, partitionOffsetString, timeoutDuration, id, state)
    }

    receivedChangeLogMessage.message.state.foreach { transaction =>
      logger.info(s"$partitionOffsetString: $transaction")
      val applyResults = stateStore.applyEntityStateChange(transaction.stateChanges)

      // if we got any KafkaCluster DELETE state-change => remove the corresponding deployment-store
      applyResults.kafkaCluster
        .collect { case ApplyStateChange.ApplyResult(Some(d: EntityState.Deleted[KafkaCluster])) => d }
        .foreach { deletedKafkaCluster =>
          logger.info(s"$partitionOffsetString: KafkaCluster#${deletedKafkaCluster.id} is deleted, removing its deployment store")
          deploymentStores.remove(KafkaClusterEntityId(deletedKafkaCluster.id))
        }

      if (!alreadyProcessed) {
        // only for new messages, we produce results
        applyResults.permission.foreach   { withStateOrNull(_) { produceStateFunc } }
        applyResults.kafkaCluster.foreach { withStateOrNull(_) { produceStateFunc } }
        applyResults.topology.foreach     { withStateOrNull(_) { produceStateFunc } }
        applyResults.deployment.foreach   { withStateOrNull(_) { produceStateFunc } }
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
        if (!alreadyProcessed) {
          // only for new messages, we produce results
          applyResults.deployedTopology.foreach { withStateOrNull(_) { produceStateFunc } }
        }
      }
    }
  }

  override protected def stopped(): Unit = producer.close()
}
