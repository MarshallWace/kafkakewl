/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package metrics

import java.time.OffsetDateTime

import com.mwam.kafkakewl.domain.kafka.KafkaTopic
import com.mwam.kafkakewl.utils.WithUtcTimestamp

final case class PartitionHighOffset(offset: Long, lastOffsetsTimestampUtc: OffsetDateTime) {
  def withLowOffset(lowOffset: Long): PartitionLowHighOffset = PartitionLowHighOffset(lowOffset, offset, lastOffsetsTimestampUtc)
  def withZeroLowOffset: PartitionLowHighOffset = withLowOffset(lowOffset = 0L)
}
object PartitionHighOffset {
  def apply(topicPartitionInfo: WithUtcTimestamp[KafkaTopic.PartitionInfo]) = new PartitionHighOffset(topicPartitionInfo.value.highOffset, topicPartitionInfo.timestampUtc)
}

final case class PartitionLowHighOffset(lowOffset: Long, highOffset: Long, lastOffsetsTimestampUtc: OffsetDateTime) {
  lazy val partitionHighOffset: PartitionHighOffset = PartitionHighOffset(highOffset, lastOffsetsTimestampUtc)
}
object PartitionLowHighOffset {
  def apply(topicPartitionInfo: WithUtcTimestamp[KafkaTopic.PartitionInfo]) = new PartitionLowHighOffset(topicPartitionInfo.value.lowOffset, topicPartitionInfo.value.highOffset, topicPartitionInfo.timestampUtc)
}

final case class ConsumerGroupOffset(offset: Long, metadata: String, lastOffsetsTimestampUtc: OffsetDateTime) {
  override def toString: String =
    if (metadata == null || metadata.isEmpty) s"$offset@${lastOffsetsTimestampUtc.toString}"
    else s"$offset['$metadata']@${lastOffsetsTimestampUtc.toString}"
}

sealed trait ConsumerGroupStatus {
  /**
    * The higher the number the more severe the status is.
    */
  def severity: Integer

  /**
    * Aggregates 2 ConsumerGroupStatuses so that the result is the more severe.
    */
  def +(other: ConsumerGroupStatus): ConsumerGroupStatus = if (severity < other.severity) other else this
}
object ConsumerGroupStatus {
  final case object Ok extends ConsumerGroupStatus { def severity = 1 }
  final case object Unknown extends ConsumerGroupStatus { def severity = 2 }
  final case object Warning extends ConsumerGroupStatus { def severity = 3 }
  final case object Error extends ConsumerGroupStatus { def severity = 4 }
  final case object MaybeStopped extends ConsumerGroupStatus { def severity = 5 }
  final case object Stopped extends ConsumerGroupStatus { def severity = 6 }

  implicit class ConsumerGroupStatusIterableExtensions(consumerGroupStatuses: Iterable[ConsumerGroupStatus]) {
    def combine: ConsumerGroupStatus = consumerGroupStatuses.fold(Ok)(_ + _)
  }
}

final case class AggregatedConsumerGroupStatus(
  best: ConsumerGroupStatus,
  worst: ConsumerGroupStatus
) {
  def +(other: AggregatedConsumerGroupStatus) =
    AggregatedConsumerGroupStatus(
      if (other.best.severity < best.severity) other.best else best,
      if (other.worst.severity > worst.severity) other.worst else worst
  )
}

object AggregatedConsumerGroupStatus {
  def apply(consumerGroupStatus: ConsumerGroupStatus) = new AggregatedConsumerGroupStatus(consumerGroupStatus, consumerGroupStatus)

  val Ok = AggregatedConsumerGroupStatus(ConsumerGroupStatus.Ok)

  implicit class AggregatedConsumerGroupStatusIterableExtensions(aggregatedConsumerGroupStatuses: Iterable[AggregatedConsumerGroupStatus]) {
    def combine: Option[AggregatedConsumerGroupStatus] = aggregatedConsumerGroupStatuses.reduceOption(_ + _)
  }
}

final case class ConsumerGroupMetrics(
  status: ConsumerGroupStatus,
  partitionHigh: Option[PartitionHighOffset],
  committed: Option[ConsumerGroupOffset],
  lag: Option[Long],
  consumedPerSecond: Option[Double]
)

object ConsumerGroupMetrics {
  implicit class ConsumerGroupMetricsExtensions(consumerGroupMetrics: Iterable[ConsumerGroupMetrics]) {
    /**
      * Calculates the total lag. It's None if ALL partition lags are None, otherwise None lags are considered 0 and the others are added up.
      */
    def sumLag: Option[Long] = if (consumerGroupMetrics.exists(_.lag.isDefined)) Some(consumerGroupMetrics.map(_.lag.getOrElse(0L)).sum) else None
    def combineStatus: ConsumerGroupStatus = consumerGroupMetrics.map(_.status).combine
    def combineStatusAggregated: Option[AggregatedConsumerGroupStatus] = consumerGroupMetrics.map(m => AggregatedConsumerGroupStatus(m.status)).combine
    def sumConsumptionRate: Option[Double] = if (consumerGroupMetrics.exists(_.consumedPerSecond.isDefined)) Some(consumerGroupMetrics.flatMap(_.consumedPerSecond).sum) else None
  }
}
