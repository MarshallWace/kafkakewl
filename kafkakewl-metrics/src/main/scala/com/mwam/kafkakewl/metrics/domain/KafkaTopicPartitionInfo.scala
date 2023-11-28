/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.domain

import org.apache.kafka.common.TopicPartition

import scala.collection.immutable.SortedMap

final case class KafkaTopicPartition(topic: String, partition: Int)

object KafkaTopicPartition {
  def apply(topicPartition: TopicPartition) = new KafkaTopicPartition(topicPartition.topic, topicPartition.partition)
}

final case class KafkaTopicPartitionInfo(beginningOffset: Long, endOffset: Long)

type KafkaSingleTopicPartitionInfos = SortedMap[Int, KafkaTopicPartitionInfo]

object KafkaSingleTopicPartitionInfos {
  val empty: SortedMap[Int, KafkaTopicPartitionInfo] = SortedMap.empty
}

type KafkaTopicPartitionInfos = Map[KafkaTopicPartition, KafkaTopicPartitionInfo]

object KafkaTopicPartitionInfos {
  val empty: Map[KafkaTopicPartition, KafkaTopicPartitionInfo] = Map.empty
}

final case class KafkaTopicPartitionInfoChanges(
    addedOrUpdated: Map[KafkaTopicPartition, KafkaTopicPartitionInfo],
    removed: Set[KafkaTopicPartition]
)

object KafkaTopicPartitionInfoExtensions {
  extension (topicPartitionInfos: Map[KafkaTopicPartition, KafkaTopicPartitionInfo]) {
    def diff(newTopicPartitionInfos: Map[KafkaTopicPartition, KafkaTopicPartitionInfo]): KafkaTopicPartitionInfoChanges = {
      val addedOrUpdated = newTopicPartitionInfos.filter { case (tp, tpi) => !topicPartitionInfos.get(tp).contains(tpi) }
      val removed = topicPartitionInfos.keySet diff newTopicPartitionInfos.keySet

      KafkaTopicPartitionInfoChanges(addedOrUpdated, removed)
    }
  }

  extension (topicInfos: Map[String, KafkaSingleTopicPartitionInfos]) {
    def applyChanges(topicInfoChanges: KafkaTopicPartitionInfoChanges): Map[String, KafkaSingleTopicPartitionInfos] = {
      val newTopicInfos = topicInfoChanges.addedOrUpdated
        .groupBy { case (tp, _) => tp.topic }
        .map { case (topic, topicPartitionInfos) =>
          (
            topic,
            SortedMap.from(topicPartitionInfos.map { case (tp, tpi) => (tp.partition, tpi) })
          )
        }

      val removedTopicPartitions = topicInfoChanges.removed
        .groupBy(tp => tp.topic)
        .map { case (topic, topicPartitionInfos) => (topic, topicPartitionInfos.map(_.partition).toSet) }

      (topicInfos.keySet union newTopicInfos.keySet).map { topic =>
        (
          topic,
          topicInfos.getOrElse(topic, KafkaSingleTopicPartitionInfos.empty)
            ++ newTopicInfos.getOrElse(topic, KafkaSingleTopicPartitionInfos.empty)
            -- removedTopicPartitions.getOrElse(topic, Set.empty)
        )
      }.toMap
    }
  }
}
