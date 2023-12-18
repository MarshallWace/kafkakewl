/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.services

import com.mwam.kafkakewl.metrics.MainConfig
import com.mwam.kafkakewl.metrics.domain.{KafkaTopicPartition, KafkaTopicPartitionInfo, KafkaTopicPartitionInfoChanges}
import zio.kafka.consumer.Consumer
import zio.{Duration, Fiber, Schedule, Task, UIO, URIO, ZIO, ZLayer}
import zio.stream.{ZSink, ZStream}
import zio.metrics.*
import zio.metrics.Metric.Counter

import scala.collection.immutable.Map

object Helper {
  def newTopicMetric(topic: String): Metric.Counter[Long] = Metric.counter("kafkakewl_vnext_topic_high_offset").tagged("topic", topic)
  def newPartitionMetric(topic: String, partition: Int): Metric.Counter[Long] =
    Metric.counter("kafkakewl_vnext_topic_partition_high_offset").tagged("topic", topic).tagged("partition", partition.toString)
}

case class MetricMap(metricMap: Map[String, (Map[Int, (Metric.Counter[Long], Long)], Metric.Counter[Long])]) {

  def updateWithTopicParitionChanges(
      changes: KafkaTopicPartitionInfoChanges,
      config: MetricExposerConfig
  ): (MetricMap, List[(Metric.Counter[Long], Long)]) = {
    val addedOrUpdated = changes.addedOrUpdated
    val removed = changes.removed
    var newMetricMap = metricMap
    var metricUpdates: List[(Metric.Counter[Long], Long)] = List.empty
    // Add new metrics and update stale ones
    for ((topicPartition, partitionInfo) <- addedOrUpdated) {

      newMetricMap = newMetricMap.updatedWith(topicPartition.topic) {
        case Some((topicMap: Map[Int, (Metric.Counter[Long], Long)], aggregateCounter: Metric.Counter[Long])) =>
          val newTopicMap = topicMap.updatedWith(topicPartition.partition) {
            case Some((counter, lastValue)) =>
              val increase = partitionInfo.endOffset - lastValue
              metricUpdates = metricUpdates :+ (aggregateCounter, increase)
              if (config.perPartitionMetrics) {
                metricUpdates = metricUpdates :+ (counter, increase)
              }
              Some((counter, partitionInfo.endOffset))
            case None =>
              val counter = Helper.newPartitionMetric(topicPartition.topic, topicPartition.partition)
              metricUpdates = metricUpdates :+ (aggregateCounter, partitionInfo.endOffset)
              if (config.perPartitionMetrics) {
                metricUpdates = metricUpdates :+ (counter, partitionInfo.endOffset)
              }
              Some((counter, partitionInfo.endOffset))
          }
          Some((newTopicMap, aggregateCounter))
        case None =>
          val counter = Helper.newPartitionMetric(topicPartition.topic, topicPartition.partition)
          val newTopicMap = Map.newBuilder
            .addOne(
              topicPartition.partition,
              (counter, partitionInfo.endOffset)
            )
            .result()
          val aggregateCounter = Helper.newTopicMetric(topicPartition.topic)
          metricUpdates = metricUpdates :+ (aggregateCounter, partitionInfo.endOffset)
          if (config.perPartitionMetrics) {
            metricUpdates = metricUpdates :+ (counter, partitionInfo.endOffset)
          }
          Some((newTopicMap, aggregateCounter))
      }
    }

    // Update the metric map to reflect removals
    for (topicPartition <- removed) {
      val topic = topicPartition.topic
      val partition = topicPartition.partition

      newMetricMap = newMetricMap.updatedWith(topic) {
        case Some((map: Map[Int, (Metric.Counter[Long], Long)], aggregateCounter: Metric.Counter[Long])) =>
          val oldMetricValue = map.get(partition).map(_._2).getOrElse(0L)
          val newMap = map.removed(partition)
          metricUpdates = metricUpdates :+ (aggregateCounter, -oldMetricValue)
          Some((newMap, aggregateCounter))
        case None => None
      }
    }

    // Clean up topics with no partitions
    newMetricMap = newMetricMap.filter((_, v) => v._1.nonEmpty)

    (MetricMap(newMetricMap), metricUpdates)
  }
}

object HighOffsetMetricsExposer {
  def startPublishing(
      topicInfos: ZStream[MetricExposerConfig, Nothing, KafkaTopicPartitionInfoChanges]
  ): URIO[MetricExposerConfig, Fiber.Runtime[Nothing, Unit]] =
    for {
      config <- ZIO.service[MetricExposerConfig]
      fibre <- topicInfos
        .runFoldZIO(MetricMap(Map.empty)) { (metricMap: MetricMap, topicPartitionInfoChanges: KafkaTopicPartitionInfoChanges) =>
          val (newMetricMap, metricUpdates) = metricMap.updateWithTopicParitionChanges(topicPartitionInfoChanges, config)
          ZIO.foreachDiscard(metricUpdates)((metric, increase) => metric.update(increase)).as(newMetricMap)
        }
        .unit
        .fork
    } yield fibre
}

case class MetricExposerConfig(perPartitionMetrics: Boolean = true)

object MetricExposerConfig {
  val live: ZLayer[MainConfig, Nothing, MetricExposerConfig] =
    ZLayer.fromZIO {
      for {
        mainConfig <- ZIO.service[MainConfig]
      } yield mainConfig.metricsExposer
    }
}
