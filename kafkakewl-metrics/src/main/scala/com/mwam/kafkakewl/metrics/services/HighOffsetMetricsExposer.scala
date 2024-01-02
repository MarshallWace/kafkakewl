/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.services

import com.mwam.kafkakewl.metrics.MainConfig
import com.mwam.kafkakewl.metrics.domain.{KafkaTopicPartition, KafkaTopicPartitionInfo, KafkaTopicPartitionInfoChanges}
import zio.{UIO, ZIO, ZLayer}
import zio.metrics.*
import zio.metrics.Metric.Counter

import scala.collection.immutable.Map

final case class MetricUpdate(metric: Metric.Counter[Long], increment: Long) {
  def applyChanges(): UIO[Unit] = {
    metric.incrementBy(increment)
  }
}

final case class PartitionMetrics(partitionMetrics: Map[Int, (Option[Metric.Counter[Long]], Long)]) {
  def updateWithNewOffset(
      newOffsets: Map[KafkaTopicPartition, KafkaTopicPartitionInfo],
      config: TopicsConfig
  ): (PartitionMetrics, List[MetricUpdate], Long) = {
    var totalChange = 0L
    var metricUpdates = List.empty[MetricUpdate]
    var newPartitionMap = partitionMetrics

    for ((topicPartition, topicPartitionInfo) <- newOffsets) {
      newPartitionMap = newPartitionMap.updatedWith(topicPartition.partition) {
        case Some((counter, lastValue)) =>
          val increase = topicPartitionInfo.endOffset - lastValue
          totalChange = totalChange + increase
          if (config.perPartitionHighOffsetMetrics) {
            metricUpdates = MetricUpdate(counter.get, increase) +: metricUpdates
          }
          Some((counter, topicPartitionInfo.endOffset))
        case None =>
          totalChange = totalChange + topicPartitionInfo.endOffset

          val counter = if (config.perPartitionHighOffsetMetrics) {
            val counter = config.newPartitionHighOffsetMetric(topicPartition.topic, topicPartition.partition)
            metricUpdates = MetricUpdate(counter, topicPartitionInfo.endOffset) +: metricUpdates
            Some(counter)
          } else {
            None
          }
          Some((counter, topicPartitionInfo.endOffset))
      }
    }

    (PartitionMetrics(newPartitionMap), metricUpdates, totalChange)
  }

  def removePartitions(topicPartitions: Set[KafkaTopicPartition], config: TopicsConfig): (PartitionMetrics, List[MetricUpdate], Long) = {
    var totalChange = 0L
    var metricUpdates = List.empty[MetricUpdate]
    var newPartitionMap = partitionMetrics

    for (topicPartition <- topicPartitions) {
      newPartitionMap = newPartitionMap.get(topicPartition.partition) match {
        case Some((counter, lastValue)) =>
          totalChange = totalChange - lastValue
          if (config.perPartitionHighOffsetMetrics) {
            metricUpdates = MetricUpdate(counter.get, -lastValue) +: metricUpdates
          }
          newPartitionMap.removed(topicPartition.partition)

        case None =>
          newPartitionMap

      }
    }

    (PartitionMetrics(newPartitionMap), metricUpdates, totalChange)
  }

  def nonEmpty: Boolean = partitionMetrics.nonEmpty
}

object PartitionMetrics {
  def empty: PartitionMetrics = PartitionMetrics(Map.empty[Int, (Option[Metric.Counter[Long]], Long)])
}

final case class MetricMap(metricMap: Map[String, (PartitionMetrics, Metric.Counter[Long])]) {

  def updateWithTopicPartitionChanges(
      changes: KafkaTopicPartitionInfoChanges,
      config: TopicsConfig
  ): (MetricMap, List[MetricUpdate]) = {
    val addedOrUpdated = changes.addedOrUpdated.groupBy(_._1.topic)
    val removed = changes.removed
    var newMetricMap = metricMap
    var metricUpdates: List[MetricUpdate] = List.empty

    // Add new metrics and update stale ones
    for ((topic, topicPartitionChanges) <- addedOrUpdated) {
      newMetricMap = newMetricMap.updatedWith(topic) { partitionMetricsAndCounter =>
        val (oldPartitionMetrics, aggregateCounter) =
          partitionMetricsAndCounter.getOrElse((PartitionMetrics.empty, config.newTopicHighOffsetMetric(topic)))

        val (newPartitionMetrics, partitionMetricUpdates, totalIncrease) = oldPartitionMetrics.updateWithNewOffset(topicPartitionChanges, config)
        metricUpdates = partitionMetricUpdates ++ metricUpdates
        metricUpdates = MetricUpdate(aggregateCounter, totalIncrease) +: metricUpdates

        Some((newPartitionMetrics, aggregateCounter))
      }
    }

    // Update the metric map to reflect removals
    for ((topic, topicPartitions) <- removed.groupBy(_.topic)) {

      newMetricMap = newMetricMap.updatedWith(topic) {
        case Some((map: PartitionMetrics, aggregateCounter: Metric.Counter[Long])) =>
          val (newPartitionMetrics, partitionMetricUpdates, totalIncrease) = map.removePartitions(topicPartitions, config)
          metricUpdates = partitionMetricUpdates ++ metricUpdates
          metricUpdates = MetricUpdate(aggregateCounter, totalIncrease) +: metricUpdates
          Some((newPartitionMetrics, aggregateCounter))
        case None => None
      }
    }

    // Clean up topics with no partitions
    newMetricMap = newMetricMap.filter((_, v) => v._1.nonEmpty)

    (MetricMap(newMetricMap), metricUpdates)
  }
}

class HighOffsetMetricsExposer

object HighOffsetMetricsExposer {
  def live: ZLayer[KafkaTopicInfoSource & TopicsConfig, Nothing, HighOffsetMetricsExposer] =
    ZLayer.scoped {
      for {
        topicInfoService <- ZIO.service[KafkaTopicInfoSource]
        topicInfoStream = topicInfoService.subscribeStream()
        config <- ZIO.service[TopicsConfig]

        offsetMetricFibre <- topicInfoStream
          .runFoldZIO(MetricMap(Map.empty)) { (metricMap: MetricMap, topicPartitionInfoChanges: KafkaTopicPartitionInfoChanges) =>
            val (newMetricMap, metricUpdates) = metricMap.updateWithTopicPartitionChanges(topicPartitionInfoChanges, config)
            ZIO.foreachDiscard(metricUpdates)(_.applyChanges()).as(newMetricMap)
          }
          .unit
          .fork
        _ <- ZIO.addFinalizer(offsetMetricFibre.interrupt)
      } yield HighOffsetMetricsExposer()
    }
}

final case class TopicsConfig(perPartitionHighOffsetMetrics: Boolean = false, metricNamePrefix: String = "kafkakewl_vnext_metrics_") {
  def newTopicHighOffsetMetric(topic: String): Metric.Counter[Long] = Metric.counter(metricNamePrefix + "topic_high_offset").tagged("topic", topic)

  def newPartitionHighOffsetMetric(topic: String, partition: Int): Metric.Counter[Long] =
    Metric.counter(metricNamePrefix + "topic_partition_high_offset").tagged("topic", topic).tagged("partition", partition.toString)
}

object TopicsConfig {
  val live: ZLayer[MainConfig, Nothing, TopicsConfig] =
    ZLayer.fromZIO {
      for {
        mainConfig <- ZIO.service[MainConfig]
      } yield mainConfig.topics
    }
}
