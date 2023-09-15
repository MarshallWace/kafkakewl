/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.domain

final case class ConsumerGroupTopicPartition(group: String, topicPartition: KafkaTopicPartition) {
  override def toString: String = s"[$group/${topicPartition.topic}/${topicPartition.partition}]"
}
object ConsumerGroupTopicPartition {
  def apply(consumerGroupId: String, topic: String, partition: Int): ConsumerGroupTopicPartition = ConsumerGroupTopicPartition(consumerGroupId, KafkaTopicPartition(topic, partition))
}
