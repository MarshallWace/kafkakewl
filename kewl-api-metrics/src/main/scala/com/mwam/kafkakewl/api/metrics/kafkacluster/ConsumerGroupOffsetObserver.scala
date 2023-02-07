/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster

import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.metrics.ConsumerGroupOffset
import com.mwam.kafkakewl.utils._
import org.apache.kafka.common.TopicPartition

final case class ConsumerGroupTopicPartition(consumerGroupId: String, topicPartition: TopicPartition) {
  override def toString: String = s"[$consumerGroupId/${topicPartition.topic}/${topicPartition.partition}]"

  /**
    * Slightly different string representation just to be able to use it in actor names
    */
  def toActorNameString: String = s"$consumerGroupId::${topicPartition.topic}::${topicPartition.partition}".toActorNameString
}
object ConsumerGroupTopicPartition {
  def apply(consumerGroupId: String, topic: String, partition: Int) = new ConsumerGroupTopicPartition(consumerGroupId, new TopicPartition(topic, partition))
}

trait ConsumerGroupOffsetObserver {
  def updateAllConsumerGroupOffsets(kafkaClusterId: KafkaClusterEntityId, offsets: Map[ConsumerGroupTopicPartition, ConsumerGroupOffset])
  def updateConsumerGroupOffsets(kafkaClusterId: KafkaClusterEntityId, offsets: Map[ConsumerGroupTopicPartition, ConsumerGroupOffset], removedOffsets: Iterable[ConsumerGroupTopicPartition])
}
