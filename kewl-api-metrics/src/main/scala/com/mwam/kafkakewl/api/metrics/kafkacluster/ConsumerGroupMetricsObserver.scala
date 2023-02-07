/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster

import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.metrics.ConsumerGroupMetrics

trait ConsumerGroupMetricsObserver {
  def removeConsumerGroupMetrics(kafkaClusterId: KafkaClusterEntityId, consumerGroupTopicPartition: ConsumerGroupTopicPartition)
  def updateConsumerGroupMetrics(kafkaClusterId: KafkaClusterEntityId, consumerGroupTopicPartition: ConsumerGroupTopicPartition, metrics: ConsumerGroupMetrics)
}
