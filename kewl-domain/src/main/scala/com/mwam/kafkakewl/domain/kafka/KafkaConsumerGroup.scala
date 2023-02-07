/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package kafka

import org.apache.kafka.common.ConsumerGroupState

import scala.collection.JavaConverters._
import scala.collection.{SortedMap, SortedSet}

object KafkaConsumerGroup {
  final case class TopicPartitionInfo(
    partition: KafkaTopic.PartitionInfo,
    committedOffset: Option[Long]
  )

  final case class TopicInfo(
    topic: String,
    partitions: SortedMap[Int, TopicPartitionInfo]
  )
  object TopicInfo {
    def apply(topic: String, partitions: Iterable[(Int, TopicPartitionInfo)]) =
      new TopicInfo(topic, SortedMap.empty[Int, TopicPartitionInfo] ++ partitions)
  }

  final case class Info(
    consumerGroup: String,
    isSimple: Boolean,
    members: Seq[KafkaConsumerGroup.Member],
    partitionAssignor: String,
    state: KafkaConsumerGroup.State,
    coordinator: KafkaNode,
    topics: SortedMap[String, TopicInfo]
  )
  object Info {
    def apply(
      consumerGroup: String,
      isSimple: Boolean,
      members: Seq[KafkaConsumerGroup.Member],
      partitionAssignor: String,
      state: KafkaConsumerGroup.State,
      coordinator: KafkaNode,
      topics: Iterable[(String, TopicInfo)]
    ) = new Info(
      consumerGroup,
      isSimple,
      members,
      partitionAssignor,
      state,
      coordinator,
      SortedMap.empty[String, TopicInfo] ++ topics
    )
  }


  sealed trait State
  object State {
    final case object Unknown extends State
    final case object PreparingRebalance extends State
    final case object CompletingRebalance extends State
    final case object Stable extends State
    final case object Dead extends State
    final case object Empty extends State

    def apply(kafkaConsumerGroupState: ConsumerGroupState): State = {
      kafkaConsumerGroupState match {
        case ConsumerGroupState.UNKNOWN => Unknown
        case ConsumerGroupState.PREPARING_REBALANCE => PreparingRebalance
        case ConsumerGroupState.COMPLETING_REBALANCE => CompletingRebalance
        case ConsumerGroupState.STABLE => Stable
        case ConsumerGroupState.DEAD => Dead
        case ConsumerGroupState.EMPTY => Empty
      }
    }
  }

  final case class Member(
    consumerId: String,
    clientId: String,
    host: String,
    assignment: SortedSet[KafkaTopic.Partition]
  )
  object Member {
    def apply(
      member: org.apache.kafka.clients.admin.MemberDescription
    ): Member = new Member(
      member.consumerId,
      member.clientId,
      member.host,
      SortedSet.empty[KafkaTopic.Partition] ++ member.assignment().topicPartitions().asScala.map(KafkaTopic.Partition.apply)
    )
  }
}
