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

// This class is purely for returning a unique type for the ZLayer below.
class HighOffsetMetricsExposer

object HighOffsetMetricsExposer {
  private final case class MetricUpdate(metric: Metric.Counter[Long], increment: Long) {
    def applyChanges(): UIO[Unit] = {
      metric.incrementBy(increment)
    }
  }

  private final case class PartitionMetrics(highOffsetCounter: Option[Metric.Counter[Long]], highOffset: Long)

  private final case class TopicPartitionsMetrics(partitionMetrics: Map[Int, PartitionMetrics]) {
    def updateWithNewOffset(
        newOffsets: Map[KafkaTopicPartition, KafkaTopicPartitionInfo],
        config: TopicsConfig
    ): (TopicPartitionsMetrics, List[MetricUpdate], Long) = {
      var totalChange = 0L
      var metricUpdates = List.empty[MetricUpdate]
      var newPartitionMap = partitionMetrics

      for ((topicPartition, topicPartitionInfo) <- newOffsets) {
        newPartitionMap = newPartitionMap.updatedWith(topicPartition.partition) {
          case Some(PartitionMetrics(counter, lastValue)) =>
            val increase = topicPartitionInfo.endOffset - lastValue
            totalChange = totalChange + increase
            if (config.perPartitionHighOffsetMetrics) {
              metricUpdates = MetricUpdate(counter.get, increase) +: metricUpdates
            }
            Some(PartitionMetrics(counter, topicPartitionInfo.endOffset))
          case None =>
            totalChange = totalChange + topicPartitionInfo.endOffset

            val counter = if (config.perPartitionHighOffsetMetrics) {
              val counter = config.newPartitionHighOffsetMetric(topicPartition.topic, topicPartition.partition)
              metricUpdates = MetricUpdate(counter, topicPartitionInfo.endOffset) +: metricUpdates
              Some(counter)
            } else {
              None
            }
            Some(PartitionMetrics(counter, topicPartitionInfo.endOffset))
        }
      }

      (TopicPartitionsMetrics(newPartitionMap), metricUpdates, totalChange)
    }

    def removePartitions(topicPartitions: Set[KafkaTopicPartition], config: TopicsConfig): (TopicPartitionsMetrics, List[MetricUpdate], Long) = {
      var totalChange = 0L
      var metricUpdates = List.empty[MetricUpdate]
      var newPartitionMap = partitionMetrics

      for (topicPartition <- topicPartitions) {
        newPartitionMap = newPartitionMap.get(topicPartition.partition) match {
          case Some(PartitionMetrics(counter, lastValue)) =>
            totalChange = totalChange - lastValue
            if (config.perPartitionHighOffsetMetrics) {
              metricUpdates = MetricUpdate(counter.get, -lastValue) +: metricUpdates
            }
            newPartitionMap.removed(topicPartition.partition)

          case None =>
            newPartitionMap

        }
      }

      (TopicPartitionsMetrics(newPartitionMap), metricUpdates, totalChange)
    }

    def nonEmpty: Boolean = partitionMetrics.nonEmpty
  }

  private object TopicPartitionsMetrics {
    val empty: TopicPartitionsMetrics = TopicPartitionsMetrics(Map.empty[Int, PartitionMetrics])
  }

  private final case class TopicMetrics(partitionMetrics: TopicPartitionsMetrics, aggregateHighOffsetCounter: Metric.Counter[Long])

  private final case class AllTopicsMetrics(topicMetrics: Map[String, TopicMetrics]) {

    def updateWithTopicPartitionChanges(
        changes: KafkaTopicPartitionInfoChanges,
        config: TopicsConfig
    ): (AllTopicsMetrics, List[MetricUpdate]) = {
      val addedOrUpdated = changes.addedOrUpdated.groupBy((topicPartition, _) => topicPartition.topic)
      val removed = changes.removed
      var newMetricMap = topicMetrics
      var metricUpdates: List[MetricUpdate] = List.empty

      // Add new metrics and update stale ones
      for ((topic, topicPartitionChanges) <- addedOrUpdated) {
        newMetricMap = newMetricMap.updatedWith(topic) { partitionMetricsAndCounter =>
          val topicMetricState =
            partitionMetricsAndCounter.getOrElse(TopicMetrics(TopicPartitionsMetrics.empty, config.newTopicHighOffsetMetric(topic)))

          val (newPartitionMetrics, partitionMetricUpdates, totalIncrease) =
            topicMetricState.partitionMetrics.updateWithNewOffset(topicPartitionChanges, config)
          metricUpdates = partitionMetricUpdates ++: metricUpdates
          metricUpdates = MetricUpdate(topicMetricState.aggregateHighOffsetCounter, totalIncrease) +: metricUpdates

          Some(TopicMetrics(newPartitionMetrics, topicMetricState.aggregateHighOffsetCounter))
        }
      }

      // Update the metric map to reflect removals
      for ((topic, topicPartitions) <- removed.groupBy(_.topic)) {

        newMetricMap = newMetricMap.updatedWith(topic) {
          case Some(map: TopicMetrics) =>
            val (newPartitionMetrics, partitionMetricUpdates, totalIncrease) = map.partitionMetrics.removePartitions(topicPartitions, config)
            metricUpdates = partitionMetricUpdates ++: metricUpdates
            metricUpdates = MetricUpdate(map.aggregateHighOffsetCounter, totalIncrease) +: metricUpdates
            Some(TopicMetrics(newPartitionMetrics, map.aggregateHighOffsetCounter))
          case None => None
        }
      }

      // Clean up topics with no partitions
      newMetricMap = newMetricMap.filter((_, v) => v.partitionMetrics.nonEmpty)

      (AllTopicsMetrics(newMetricMap), metricUpdates)
    }
  }
  def live: ZLayer[KafkaTopicInfoSource & TopicsConfig, Nothing, HighOffsetMetricsExposer] =
    ZLayer.scoped {
      for {
        topicInfoService <- ZIO.service[KafkaTopicInfoSource]
        topicInfoStream = topicInfoService.subscribeStream()
        config <- ZIO.service[TopicsConfig]

        offsetMetricFibre <- topicInfoStream
          .runFoldZIO(AllTopicsMetrics(Map.empty)) { (metricMap: AllTopicsMetrics, topicPartitionInfoChanges: KafkaTopicPartitionInfoChanges) =>
            val (newMetricMap, metricUpdates) = metricMap.updateWithTopicPartitionChanges(topicPartitionInfoChanges, config)
            ZIO.foreachDiscard(metricUpdates)(_.applyChanges()).as(newMetricMap)
          }
          .unit
          .fork
        _ <- ZIO.addFinalizer(offsetMetricFibre.interrupt)
      } yield HighOffsetMetricsExposer()
    }
}

final case class TopicsConfig(perPartitionHighOffsetMetrics: Boolean = true, metricNamePrefix: String = "kafkakewl_vnext_") {
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
