/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.stateadaptor.destinations

import com.mwam.kafkakewl.kafka.utils.{KafkaConfigProperties, KafkaConnection, KafkaProducerConfig}
import com.mwam.kafkakewl.common.AllDeploymentEntities
import com.mwam.kafkakewl.common.AllDeploymentEntities.InMemoryStateStores._
import com.mwam.kafkakewl.common.AllDeploymentEntities.WritableStateStores._
import com.mwam.kafkakewl.common.changelog.ReceivedChangeLogMessage
import com.mwam.kafkakewl.domain.EntityState
import com.mwam.kafkakewl.domain.JsonEncodeDecode._
import com.mwam.kafkakewl.domain.deploy.DeployedTopology
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.stateadaptor.StateAdaptorStateful
import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.producer.KafkaProducer
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContextExecutor

object LatestDeployedTopologyStateAdaptor {
  final case class Config(changeLogKafkaConsumerGroup: String, kafkaClusterId: KafkaClusterEntityId, destinationKafkaConnection: KafkaConnection, destinationTopicName: String)
}

class LatestDeployedTopologyStateAdaptor(
  changeLogKafkaConnection: KafkaConnection,
  config: LatestDeployedTopologyStateAdaptor.Config,
  timeoutDuration: Duration
)(implicit
  ec: ExecutionContextExecutor
) extends StateAdaptorStateful(changeLogKafkaConnection, config.changeLogKafkaConsumerGroup) {

  override protected lazy val logger: Logger = Logger(LoggerFactory.getLogger(getClass.getName + s"-${config.kafkaClusterId}"))

  private val stateStore = AllDeploymentEntities.InMemoryStateStores().toWritable

  private val producer = new KafkaProducer[String, String](KafkaConfigProperties.forProducer(KafkaProducerConfig(config.destinationKafkaConnection, idempotent = Some(true))))

  override protected def handleChangeLogMessage(receivedChangeLogMessage: ReceivedChangeLogMessage, alreadyProcessed: Boolean): Unit = {
    // filtering for the kafka-cluster
    val partitionOffsetString = receivedChangeLogMessage.partitionOffsetString + " " + (if (alreadyProcessed) "reprocessing" else "new")
    if (receivedChangeLogMessage.message.kafkaClusterId.contains(config.kafkaClusterId)) {
      val produceStateFunc = produceState[DeployedTopology](producer, config.destinationTopicName, partitionOffsetString, timeoutDuration, _: String, _: EntityState.Live[DeployedTopology])
      logger.info(s"$partitionOffsetString: $receivedChangeLogMessage")
      receivedChangeLogMessage.message.deployment.foreach { d =>
        // applying all changes (even ones already processed so that we build up the current state)
        val applyResults = stateStore.applyEntityStateChange(d.stateChanges)
        if (!alreadyProcessed) {
          // only for new messages, we produce results
          applyResults.deployedTopology.foreach { withStateOrNull(_) { produceStateFunc } }
        }
      }
    } else {
      logger.info(s"$partitionOffsetString: skipping message (kafkaCluster: ${receivedChangeLogMessage.message.kafkaClusterId.getOrElse("none")})")
    }
  }

  override protected def stopped(): Unit = producer.close()
}
