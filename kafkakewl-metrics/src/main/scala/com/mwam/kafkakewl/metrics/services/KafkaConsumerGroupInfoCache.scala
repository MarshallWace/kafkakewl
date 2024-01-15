/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.services

import com.mwam.kafkakewl.metrics.domain.{KafkaConsumerGroupMetrics, KafkaConsumerGroupInfo, KafkaConsumerGroupTopicInfo}
import zio.*

import scala.collection.immutable.SortedMap

class KafkaConsumerGroupInfoCache(
    private val consumerGroupsMetricsByGroupByPartitionRef: Ref[Map[String, KafkaConsumerGroupInfo]]
) {
  def getConsumerGroups: UIO[Seq[String]] = consumerGroupsMetricsByGroupByPartitionRef.get.map(_.keys.toSeq)

  def getConsumerGroupMetrics(consumerGroup: String): UIO[Option[KafkaConsumerGroupInfo]] =
    consumerGroupsMetricsByGroupByPartitionRef.get.map(_.get(consumerGroup))

  def getConsumerGroupTopicMetrics(consumerGroup: String, topic: String): UIO[Option[KafkaConsumerGroupTopicInfo]] =
    consumerGroupsMetricsByGroupByPartitionRef.get.map(_.get(consumerGroup).flatMap(_.topics.get(topic)))

  def getConsumerGroupsMetrics(consumerGroups: Iterable[String]): UIO[Map[String, Option[KafkaConsumerGroupInfo]]] =
    consumerGroupsMetricsByGroupByPartitionRef.get.map(consumerGroupsMetricsByGroup =>
      consumerGroups.map(group => (group, consumerGroupsMetricsByGroup.get(group))).toMap
    )
}

object KafkaConsumerGroupInfoCache {
  def live: ZLayer[KafkaConsumerGroupMetricsCalc, Nothing, KafkaConsumerGroupInfoCache] =
    ZLayer.scoped {
      for {
        metricChangesDequeue <- ZIO.serviceWithZIO[KafkaConsumerGroupMetricsCalc](_.subscribe())
        consumerGroupsMetricsByGroupByPartitionRef <- Ref.make(Map.empty[String, KafkaConsumerGroupInfo])
        processMetricChangesFiber <- processMetricChanges(
          metricChangesDequeue,
          consumerGroupsMetricsByGroupByPartitionRef
        ).forever.fork
        _ <- ZIO.addFinalizer(processMetricChangesFiber.interrupt)
      } yield KafkaConsumerGroupInfoCache(consumerGroupsMetricsByGroupByPartitionRef)
    }

  private def processMetricChanges(
      metricChangesDequeue: Dequeue[KafkaConsumerGroupMetricChanges],
      consumerGroupsMetricsByGroupByPartitionRef: Ref[Map[String, KafkaConsumerGroupInfo]]
  ) = for {
    consumerGroupMetricChanges <- metricChangesDequeue.take

    // We nest the updates to be the structure Group -> Topic -> Partition -> KafkaConsumerGroupMetrics
    addedConsumerGroupMetricChangesGrouped = consumerGroupMetricChanges.addedOrUpdated
      .groupBy((consumerGroupTopicPartition, _) => consumerGroupTopicPartition.group)
      .map { (group, groupMetrics) =>
        val topicMap = KafkaConsumerGroupInfo(
          SortedMap.from(
            groupMetrics
              .groupBy((consumerGroupTopicPartition, _) => consumerGroupTopicPartition.topicPartition.topic)
              .map { (topic, groupMetrics) =>
                val value = KafkaConsumerGroupTopicInfo(
                  SortedMap.from(groupMetrics.map((partition, groupMetrics) => (partition.topicPartition.partition, groupMetrics)))
                )
                (topic, value)
              }
          )
        )
        (group, topicMap)
      }

    // We nest the removals to be the structure Group -> Topic -> Set[Partition]
    removedConsumerGroupMetricChangesGrouped = consumerGroupMetricChanges.removed
      .groupBy(consumerGroupTopicPartition => consumerGroupTopicPartition.group)
      .map { (group, groupMetrics) =>
        val topicMap = SortedMap.from(
          groupMetrics
            .groupBy(consumerGroupTopicPartition => consumerGroupTopicPartition.topicPartition.topic)
            .map((topic, partition) => (topic, partition.map(_.topicPartition.partition)))
        )
        (group, topicMap)
      }
    _ <- consumerGroupsMetricsByGroupByPartitionRef.update { oldMap =>
      deepMergeRemove(deepMergeAdd(oldMap, addedConsumerGroupMetricChangesGrouped), removedConsumerGroupMetricChangesGrouped)
    }
  } yield ()

  private def deepMergeAdd(
      metricMap: Map[String, KafkaConsumerGroupInfo],
      addedOrUpdated: Map[String, KafkaConsumerGroupInfo]
  ): Map[String, KafkaConsumerGroupInfo] = {
    metricMap.keySet
      .union(addedOrUpdated.keySet)
      .map(group =>
        val newTopicMap = (metricMap.get(group), addedOrUpdated.get(group)) match {
          case (Some(oldTopicMap), Some(newTopicMap)) =>
            val updatedTopicMap = oldTopicMap.topics.keySet.unsorted
              .union(newTopicMap.topics.keySet.unsorted)
              .map(topic =>
                val newPartitionMap = (oldTopicMap.topics.get(topic), newTopicMap.topics.get(topic)) match {
                  case (Some(oldTopicMap), Some(newTopicMap)) => oldTopicMap.partitions ++ newTopicMap.partitions
                  case (oldPartitionMap, newPartitionMap)     => oldPartitionMap.map(_.partitions).orElse(newPartitionMap.map(_.partitions)).get
                }
                (topic, KafkaConsumerGroupTopicInfo(newPartitionMap))
              )
            SortedMap.from(updatedTopicMap)
          case (oldTopicMap, newTopicMap) => oldTopicMap.map(_.topics).orElse(newTopicMap.map(_.topics)).get
        }
        (group, KafkaConsumerGroupInfo(newTopicMap))
      )
      .toMap
  }

  private def deepMergeRemove(
      metricMap: Map[String, KafkaConsumerGroupInfo],
      removed: Map[String, SortedMap[String, Set[Int]]]
  ): Map[String, KafkaConsumerGroupInfo] = {
    metricMap.keySet
      .union(removed.keySet)
      .map(group =>
        val newTopicMap = (metricMap.get(group), removed.get(group)) match {
          case (Some(oldTopicMap), Some(removedTopicMap)) =>
            val updatedTopicMap = oldTopicMap.topics.keySet.unsorted
              .union(removedTopicMap.keySet)
              .map(topic =>
                val newPartitionMap = (oldTopicMap.topics.get(topic), removedTopicMap.get(topic)) match {
                  case (Some(oldPartitionMap), Some(removedPartitionSet)) => oldPartitionMap.partitions.removedAll(removedPartitionSet)
                  case (Some(oldPartitionMap), _)                         => oldPartitionMap.partitions
                  case (_, _)                                             => SortedMap.empty[Int, KafkaConsumerGroupMetrics]
                }
                (topic, KafkaConsumerGroupTopicInfo(newPartitionMap))
              )
            SortedMap.from(updatedTopicMap)

          case (Some(oldTopicMap), _) => oldTopicMap.topics
          case (_, _)                 => SortedMap.empty[String, KafkaConsumerGroupTopicInfo]
        }
        (group, KafkaConsumerGroupInfo(newTopicMap))
      )
      .toMap
  }

}
