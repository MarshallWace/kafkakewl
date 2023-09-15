/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.domain

import scala.collection.immutable.SortedMap

final case class KafkaConsumerGroupInfo(
  topics: Map[String, SortedMap[Int, KafkaConsumerGroupOffset]]
)

object KafkaConsumerGroupInfo
{
  val empty: KafkaConsumerGroupInfo = KafkaConsumerGroupInfo(Map.empty)
}

object KafkaConsumerGroupInfoExtensions {
  extension (consumerGroupInfos: Map[String, KafkaConsumerGroupInfo]) {
    def applyChanges(consumerGroupOffsets: KafkaConsumerGroupOffsets): Map[String, KafkaConsumerGroupInfo] = {
      // TODO may not be very efficient, but works.
      consumerGroupOffsets.foldLeft(consumerGroupInfos) { case (newConsumerGroupInfos, (cgtp, cgo)) =>
        val group = cgtp.group
        val topic = cgtp.topicPartition.topic
        val partition = cgtp.topicPartition.partition

        val currentConsumerGroupInfo = consumerGroupInfos.getOrElse(group, KafkaConsumerGroupInfo.empty)
        val currentConsumerGroupTopic = currentConsumerGroupInfo.topics.getOrElse(topic, SortedMap.empty[Int, KafkaConsumerGroupOffset])

        val newConsumerGroupTopic = cgo match {
          case Some(consumerGroupOffset) => currentConsumerGroupTopic + (partition -> consumerGroupOffset)
          case None => currentConsumerGroupTopic - partition
        }
        val newConsumerGroupInfo = KafkaConsumerGroupInfo(currentConsumerGroupInfo.topics + (topic -> newConsumerGroupTopic))
        newConsumerGroupInfos + (cgtp.group -> newConsumerGroupInfo)
      }
    }
  }
}
