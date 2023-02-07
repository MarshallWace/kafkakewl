/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package kafka

import java.time.OffsetDateTime

import com.mwam.kafkakewl.utils._

import scala.collection.SortedMap
import scala.concurrent.duration.Duration

object KafkaTopic {
  final case class Partition(topic: String, partition: Int)
  object Partition {
    implicit val ordering: Ordering[Partition] = Ordering.by[Partition, (String, Int)](tp => (tp.topic, tp.partition))
    def apply(topicPartition: org.apache.kafka.common.TopicPartition) = new Partition(topicPartition.topic, topicPartition.partition)

    implicit class KafkaTopicPartitionsExtensions(topicPartitions: Iterable[KafkaTopic.Partition]) {
      def toPrettyString: String =
        topicPartitions
          .toSeq
          .groupBy(_.topic)
          .map { case (topic, tps) =>
            (topic,
              tps
                .map(_.partition)
                .sorted
                .map(p => s"P=$p")
                .mkString(", ")
            )
          }
          .toSeq.sortBy { case (t, _) => t }
          .map { case (t, d) => s"$t: $d"}
          .mkString("; ")
    }
  }

  final case class Metrics(
    incomingMessagesPerSecond: Double,
    lastOffsetTimestampUtc: OffsetDateTime
  )
  object Metrics {
    def apply(
      duration: Duration,
      incomingMessages: Long,
      lastOffsetTimestampUtc: OffsetDateTime
    ): Metrics = {
      def createMetrics(incomingMessagesPerSecond: Double) =
        new Metrics(incomingMessagesPerSecond, lastOffsetTimestampUtc)

      if (duration.toMillis != 0)
        createMetrics(incomingMessages / (duration.toMillis / 1000.0))
      else
        createMetrics(0)
    }
  }
  final case class PartitionInfo(lowOffset: Long, highOffset: Long, metrics: Option[Metrics] = None) {
    def withMetrics(metrics: Metrics): PartitionInfo = copy(metrics = Some(metrics))
    def numberOfOffsets: Long = highOffset - lowOffset
  }
  final case class Info(topic: String, partitions: SortedMap[Int, PartitionInfo], metrics: Option[Metrics] = None) {
    def withMetrics(topicMetrics: Option[Metrics], partitionsMetrics: SortedMap[Int, Metrics]): Info =
      copy(
        metrics = topicMetrics,
        partitions = partitions.map { case (p, pi) => (p, pi.copy(metrics = partitionsMetrics.get(p))) }
      )
  }
  object Info {
    def apply(topic: String, partitions: Iterable[(Int, PartitionInfo)], metrics: Option[Metrics]) =
      new Info(topic, partitions.toSortedMap, metrics)
  }
}
