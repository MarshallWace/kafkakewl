/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package metrics

import com.mwam.kafkakewl.domain.deploy.DeployedTopologyEntityId
import com.mwam.kafkakewl.domain.kafka.KafkaTopic
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaClusterAndTopology, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.topology._

import scala.collection.SortedMap

final case class DeployedTopologyMetricsCompact(
  kafkaClusterId: KafkaClusterEntityId,
  topologyId: TopologyEntityId,
  aggregateConsumerGroupStatus: ConsumerGroupStatus,
  aggregatedConsumerGroupStatus: Option[AggregatedConsumerGroupStatus]
) extends Entity {
  def id = DeployedTopologyEntityId(KafkaClusterAndTopology.id(kafkaClusterId, topologyId))
}

final case class DeployedTopologyMetrics(
  kafkaClusterId: KafkaClusterEntityId,
  topologyId: TopologyEntityId,
  aggregateConsumerGroupStatus: ConsumerGroupStatus,
  aggregatedConsumerGroupStatus: Option[AggregatedConsumerGroupStatus],
  topics: Map[TopicId, Option[KafkaTopic.Info]],
  applications: Map[ApplicationId, Option[DeployedTopologyMetrics.Application.Info]],
) extends Entity {
  def id = DeployedTopologyEntityId(KafkaClusterAndTopology.id(kafkaClusterId, topologyId))
}

object DeployedTopologyMetrics {
  def compact(dtm: DeployedTopologyMetrics): DeployedTopologyMetricsCompact = {
    DeployedTopologyMetricsCompact(
      dtm.kafkaClusterId,
      dtm.topologyId,
      dtm.aggregateConsumerGroupStatus,
      dtm.aggregatedConsumerGroupStatus
    )
  }

  object Application {
    object ConsumerGroup {
      object Topic {
        final case class Info(
          aggregateConsumerGroupStatus: ConsumerGroupStatus,
          aggregatedConsumerGroupStatus: Option[AggregatedConsumerGroupStatus],
          totalLag: Option[Long],
          partitions: SortedMap[Int, ConsumerGroupMetrics],
          totalConsumedPerSecond: Option[Double]
        )
      }

      final case class Info(
        aggregateConsumerGroupStatus: ConsumerGroupStatus,
        aggregatedConsumerGroupStatus: Option[AggregatedConsumerGroupStatus],
        topics: Map[TopicId, Topic.Info]
      )
    }

    final case class Info(
      consumerGroupStatus: Option[ConsumerGroup.Info] = None,
    )
  }
}