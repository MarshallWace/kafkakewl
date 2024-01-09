/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.domain

enum ConsumerGroupStatus(val severity: Int):
  /** Aggregates 2 ConsumerGroupStatuses so that the result is the more severe.
    */
  def +(other: ConsumerGroupStatus): ConsumerGroupStatus = if (severity < other.severity) other else this

  case Ok extends ConsumerGroupStatus(1)
  case Unknown extends ConsumerGroupStatus(2)
  case Warning extends ConsumerGroupStatus(3)
  case Error extends ConsumerGroupStatus(4)
  case MaybeStopped extends ConsumerGroupStatus(5)
  case Stopped extends ConsumerGroupStatus(6)

end ConsumerGroupStatus

implicit class ConsumerGroupStatusIterableExtensions(consumerGroupStatuses: Iterable[ConsumerGroupStatus]) {
  def combine: ConsumerGroupStatus = consumerGroupStatuses.fold(ConsumerGroupStatus.Ok)(_ + _)
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

final case class KafkaConsumerGroupMetrics(
    status: ConsumerGroupStatus,
    partitionHigh: Option[KafkaTopicPartitionInfo],
    committed: Option[KafkaConsumerGroupOffset],
    lag: Option[Long],
    consumedPerSecond: Option[Double]
)

object KafkaConsumerGroupMetrics {
  implicit class ConsumerGroupMetricsExtensions(consumerGroupMetrics: Iterable[KafkaConsumerGroupMetrics]) {

    /** Calculates the total lag. It's None if ALL partition lags are None, otherwise None lags are considered 0 and the others are added up.
      */
    def sumLag: Option[Long] = if (consumerGroupMetrics.exists(_.lag.isDefined)) Some(consumerGroupMetrics.map(_.lag.getOrElse(0L)).sum) else None

    def combineStatus: ConsumerGroupStatus = consumerGroupMetrics.map(_.status).combine

    def combineStatusAggregated: Option[AggregatedConsumerGroupStatus] =
      consumerGroupMetrics.map(m => AggregatedConsumerGroupStatus(m.status)).combine

    def sumConsumptionRate: Option[Double] =
      if (consumerGroupMetrics.exists(_.consumedPerSecond.isDefined)) Some(consumerGroupMetrics.flatMap(_.consumedPerSecond).sum) else None
  }
}
