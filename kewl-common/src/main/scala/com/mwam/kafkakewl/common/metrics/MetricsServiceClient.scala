/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.metrics

import com.mwam.kafkakewl.domain.CommandError
import com.mwam.kafkakewl.domain.kafka.KafkaTopic
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.metrics.TopicPartitionsConsumerGroupMetrics
import com.mwam.kafkakewl.domain.topology.TopologyToDeploy

import scala.collection.SortedMap

/**
  * Trait for retrieving topics and group metrics from the metrics http-service.
  */
trait MetricsServiceClient {
  def getTopicInfos(
    allCurrentDeployedTopologiesByKafkaClusterId: Map[KafkaClusterEntityId, Iterable[TopologyToDeploy]],
    deployedTopologiesByKafkaClusterId: Map[KafkaClusterEntityId, Iterable[TopologyToDeploy]]
  ): Map[KafkaClusterEntityId, SortedMap[String, Either[CommandError, KafkaTopic.Info]]]

  def getConsumerGroupMetrics(
    allCurrentDeployedTopologiesByKafkaClusterId: Map[KafkaClusterEntityId, Iterable[TopologyToDeploy]],
    deployedTopologiesByKafkaClusterId: Map[KafkaClusterEntityId, Iterable[TopologyToDeploy]]
  ): Map[KafkaClusterEntityId, SortedMap[String, Either[CommandError, TopicPartitionsConsumerGroupMetrics]]]
}
