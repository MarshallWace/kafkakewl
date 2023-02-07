/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster

import com.mwam.kafkakewl.kafka.utils.KafkaConnection
import com.mwam.kafkakewl.api.metrics.kafkacluster.groups.KafkaClusterConsumerGroupOffsetsCollector
import com.mwam.kafkakewl.api.metrics.kafkacluster.topics.KafkaTopicMetricsCollector
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContextExecutor

object KafkaClusterMetricsCollector {
  final case class Observers(
    kafkaClusterObserver: KafkaClusterObserver,
    topicInfoObserver: TopicInfoObserver,
    consumerGroupOffsetObserver: ConsumerGroupOffsetObserver
  )

  final case class Config(
    createKafkaTopicMetricsCollectorFunc: (KafkaClusterMetricsHelper, TopicInfoObserver) => KafkaTopicMetricsCollector,
    createKafkaClusterConsumerGroupOffsetsCollectorFunc: (KafkaClusterMetricsHelper, ConsumerGroupOffsetObserver) => KafkaClusterConsumerGroupOffsetsCollector
  )
}

/**
  * A kafka metrics collector for a particular kafka-cluster.
  *
  * @param kafkaClusterId the id of the kafka-cluster
  * @param connection the connection to the kafka-cluster
  * @param observers the observers
  * @param config misc. collector configuration
  */
class KafkaClusterMetricsCollector(
  kafkaClusterId: KafkaClusterEntityId,
  connection: KafkaConnection,
  observers: KafkaClusterMetricsCollector.Observers,
  config: KafkaClusterMetricsCollector.Config
)(implicit ec: ExecutionContextExecutor) extends LazyLogging {

  logger.info(s"created for $connection")

  private val helper = new KafkaClusterMetricsHelper(connection, kafkaClusterId)
  private val topicMetricsCollector = config.createKafkaTopicMetricsCollectorFunc(helper, observers.topicInfoObserver)
  private val groupConsumerGroupOffsetsCollector = config.createKafkaClusterConsumerGroupOffsetsCollectorFunc(helper, observers.consumerGroupOffsetObserver)

  def close(): Unit = {
    logger.info(s"closing")

    topicMetricsCollector.stop()
    groupConsumerGroupOffsetsCollector.stop()
    helper.close()

    // this comes last so that the collectors won't send notifications to the observers, and the kafka-cluster doesn't come back
    observers.kafkaClusterObserver.remove(kafkaClusterId)
    logger.info(s"closed")
  }
}
