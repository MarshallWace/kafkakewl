/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster.observers

import com.mwam.kafkakewl.api.metrics.kafkacluster.{ConsumerGroupMetricsObserver, ConsumerGroupTopicPartition, KafkaClusterObserver}
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.metrics.{ConsumerGroupMetrics, ConsumerGroupStatus, TopicPartitionsConsumerGroupMetrics}
import com.mwam.kafkakewl.utils._
import nl.grons.metrics4.scala.{DefaultInstrumented, MetricName}

import scala.collection.{SortedMap, concurrent}

trait ConsumerGroupMetricsCache {
  def getConsumerGroups(kafkaClusterId: KafkaClusterEntityId): CacheResult[Iterable[String]]
  def getConsumerGroupMetrics(kafkaClusterId: KafkaClusterEntityId, consumerGroup: String): CacheResult[TopicPartitionsConsumerGroupMetrics]
  def getConsumerGroupTopicMetrics(kafkaClusterId: KafkaClusterEntityId, consumerGroup: String, topic: String): CacheResult[SortedMap[Int, ConsumerGroupMetrics]]

  def getConsumerGroupsMetrics(kafkaClusterId: KafkaClusterEntityId, consumerGroups: Iterable[String]): SortedMap[String, CacheResult[TopicPartitionsConsumerGroupMetrics]] = {
    consumerGroups.map(cg => (cg, getConsumerGroupMetrics(kafkaClusterId, cg))).toSortedMap
  }
}

class ConsumerGroupMetricsCacheImpl extends ConsumerGroupMetricsCache
  with KafkaClusterObserver
  with ConsumerGroupMetricsObserver
  with DefaultInstrumented {

  override lazy val metricBaseName = MetricName("com.mwam.kafkakewl.api.metrics.consumer")

  private val consumerGroupMetrics = concurrent.TrieMap.empty[KafkaClusterEntityId, concurrent.TrieMap[ConsumerGroupTopicPartition, ConsumerGroupMetrics]]
  private val consumerGroupMetricsByConsumerGroupAndTopic = concurrent.TrieMap.empty[KafkaClusterEntityId, SortedMap[String, SortedMap[String, Map[ConsumerGroupTopicPartition, ConsumerGroupMetrics]]]]

  private def updateConsumerGroupMetricsByConsumerGroup(
    kafkaClusterId: KafkaClusterEntityId,
    consumerGroupsMetrics: concurrent.TrieMap[ConsumerGroupTopicPartition, ConsumerGroupMetrics]
  ): Unit = {
    // invalidating the consumer group metrics by consumer group
    invalidateConsumerGroupMetricsByConsumerGroupAndTopic(kafkaClusterId)
    // NOT recalculating it, it'll be done on-demand when anybody requests it
    //getConsumerGroupMetricsByConsumerGroupAndTopic(kafkaClusterId)
  }

  private def calculateConsumerGroupMetricsByConsumerGroupAndTopic(consumerGroupsMetrics: concurrent.TrieMap[ConsumerGroupTopicPartition, ConsumerGroupMetrics]) =
    consumerGroupsMetrics
      // first group by consumer-group
      .groupBy { case (cgtp, cgm) => cgtp.consumerGroupId }
      // then every group is further grouped by topic
      .mapValues(_.groupBy { case (cgtp, cgm) => cgtp.topicPartition.topic }.mapValues(_.toMap).toSortedMap)
      .toSortedMap

  private def invalidateConsumerGroupMetricsByConsumerGroupAndTopic(kafkaClusterId: KafkaClusterEntityId) = {
    consumerGroupMetricsByConsumerGroupAndTopic.remove(kafkaClusterId)
  }

  private def getConsumerGroupMetricsByConsumerGroupAndTopic(kafkaClusterId: KafkaClusterEntityId) = {
    consumerGroupMetricsByConsumerGroupAndTopic
      .get(kafkaClusterId)
      .map(Some(_))
      .getOrElse {
        val currentConsumerGroupMetricsByConsumerGroupAndTopic = consumerGroupMetrics
          .get(kafkaClusterId)
          .map(calculateConsumerGroupMetricsByConsumerGroupAndTopic)
        // update the cache if we have a value
        currentConsumerGroupMetricsByConsumerGroupAndTopic.foreach(consumerGroupMetricsByConsumerGroupAndTopic.update(kafkaClusterId, _))
        // finally return the value or empty
        currentConsumerGroupMetricsByConsumerGroupAndTopic
      }
  }

  private def getCurrentTopicConsumerGroupMetrics(kafkaClusterId: KafkaClusterEntityId, gaugeName: String, group: String, topic: String): Option[Map[ConsumerGroupTopicPartition, ConsumerGroupMetrics]] =
    getConsumerGroupMetricsByConsumerGroupAndTopic(kafkaClusterId)
      .flatMap(_.get(group))
      .flatMap(_.get(topic))

  private def getCurrentTopicTotalLagForGaugeMetrics(kafkaClusterId: KafkaClusterEntityId, gaugeName: String, group: String, topic: String): Option[Long] =
    getCurrentTopicConsumerGroupMetrics(kafkaClusterId, gaugeName, group, topic)
      .map(_.values
        // the lag defaults to the high-offset, if that's missing, it's just zero
        .map { cgm => cgm.lag.orElse(cgm.partitionHigh.map(_.offset)).getOrElse(0L) }
        .sum
      )

  private def getCurrentTopicTotalConsumedForGaugeMetrics(kafkaClusterId: KafkaClusterEntityId, gaugeName: String, group: String, topic: String): Option[Double] =
    getConsumerGroupMetricsByConsumerGroupAndTopic(kafkaClusterId)
      .flatMap(_.get(group))
      .flatMap(_.get(topic))
      .map(_.values.sumConsumptionRate.getOrElse(0.0))

  private def getCurrentTopicConsumerStatusForGaugeMetrics(kafkaClusterId: KafkaClusterEntityId, gaugeName: String, group: String, topic: String): Option[Int] =
    getCurrentTopicConsumerGroupMetrics(kafkaClusterId, gaugeName, group, topic)
      .map(_.values.combineStatus.severity)

  private def getCurrentPartitionLagForGaugeMetrics(kafkaClusterId: KafkaClusterEntityId, gaugeName: String, group: String, topic: String, partition: Int): Option[Long] = {
    consumerGroupMetrics.get(kafkaClusterId)
      .flatMap(_.get(ConsumerGroupTopicPartition(group, topic, partition)))
      // the lag defaults to the high-offset, if that's missing, it's just zero
      .map(cgm => cgm.lag.orElse(cgm.partitionHigh.map(_.offset)).getOrElse(0L))
  }

  private def updateTopicAndPartitionMetrics(
    kafkaClusterId: KafkaClusterEntityId,
    consumerGroupTopicPartition: ConsumerGroupTopicPartition
  ): Unit = {
    val group = consumerGroupTopicPartition.consumerGroupId
    val topic = consumerGroupTopicPartition.topicPartition.topic
    val partition = consumerGroupTopicPartition.topicPartition.partition

    val lag = "lag"
    val consumedRate = "consumedrate"
    val consumerStatus = "consumerstatus"
    def partitionGaugeNameOf(gaugeName: String) = s"$kafkaClusterId:$gaugeName:$group:$topic:$partition"
    def topicGaugeNameOf(gaugeName: String) = s"$kafkaClusterId:$gaugeName:$group:$topic:all"

    // creating the gauge metrics if they are not there yet (removal happens when the gauge can't provide any value (because e.g. the kafka-cluster or group or topic is no longer there))

    // for not we don't publish per-partition data, it's too many gauges
//    val lagPartitionGaugeName = partitionGaugeNameOf(lag)
//    metrics.createGaugeIfDoesNotExist(lagPartitionGaugeName, defaultValue = 0L)(
//      getCurrentPartitionLagForGaugeMetrics(kafkaClusterId, lagPartitionGaugeName, group, topic, partition)
//    )

    val lagTopicGaugeName = topicGaugeNameOf(lag)
    metrics.createGaugeIfDoesNotExistFast(lagTopicGaugeName, defaultValue = 0L)(
      getCurrentTopicTotalLagForGaugeMetrics(kafkaClusterId, lagTopicGaugeName, group, topic)
    )

    val consumedRateTopicGaugeName = topicGaugeNameOf(consumedRate)
    metrics.createGaugeIfDoesNotExistFast(consumedRateTopicGaugeName, defaultValue = 0.0)(
      getCurrentTopicTotalConsumedForGaugeMetrics(kafkaClusterId, consumedRateTopicGaugeName, group, topic)
    )

    val consumerStatusTopicGaugeName = topicGaugeNameOf(consumerStatus)
    metrics.createGaugeIfDoesNotExistFast(consumerStatusTopicGaugeName, defaultValue = ConsumerGroupStatus.Unknown.severity)(
      getCurrentTopicConsumerStatusForGaugeMetrics(kafkaClusterId, consumerStatusTopicGaugeName, group, topic)
    )
  }

  override def remove(kafkaClusterId: KafkaClusterEntityId): Unit = {
    consumerGroupMetrics.remove(kafkaClusterId)
    invalidateConsumerGroupMetricsByConsumerGroupAndTopic(kafkaClusterId)
  }

  override def removeConsumerGroupMetrics(kafkaClusterId: KafkaClusterEntityId, consumerGroupTopicPartition: ConsumerGroupTopicPartition): Unit = {
    for {
      consumerGroupsMetrics <- consumerGroupMetrics.get(kafkaClusterId)
    } yield {
      consumerGroupsMetrics.remove(consumerGroupTopicPartition)

      // as a result, need to update the other data-structure too
      updateConsumerGroupMetricsByConsumerGroup(kafkaClusterId, consumerGroupsMetrics)
    }
  }

  override def updateConsumerGroupMetrics(kafkaClusterId: KafkaClusterEntityId, consumerGroupTopicPartition: ConsumerGroupTopicPartition, metrics: ConsumerGroupMetrics): Unit = {
    val consumerGroupsMetrics = consumerGroupMetrics.getOrElseUpdate(kafkaClusterId, concurrent.TrieMap.empty[ConsumerGroupTopicPartition, ConsumerGroupMetrics])
    consumerGroupsMetrics.update(consumerGroupTopicPartition, metrics)

    // as a result, need to update the other data-structure too
    updateConsumerGroupMetricsByConsumerGroup(kafkaClusterId, consumerGroupsMetrics)

    // publish it as metrics
    updateTopicAndPartitionMetrics(kafkaClusterId, consumerGroupTopicPartition)
  }

  def getConsumerGroups(kafkaClusterId: KafkaClusterEntityId): CacheResult[Iterable[String]] = {
    for {
      consumerGroupsMetricsByConsumerGroup <- getConsumerGroupMetricsByConsumerGroupAndTopic(kafkaClusterId).toRight(CacheError.NoConsumerGroupMetricsForKafkaCluster)
    } yield consumerGroupsMetricsByConsumerGroup.keys
  }

  def getConsumerGroupMetrics(kafkaClusterId: KafkaClusterEntityId, consumerGroup: String): CacheResult[TopicPartitionsConsumerGroupMetrics] = {
    for {
      consumerGroupsMetricsByConsumerGroup <- getConsumerGroupMetricsByConsumerGroupAndTopic(kafkaClusterId).toRight(CacheError.NoConsumerGroupMetricsForKafkaCluster)
      consumerGroupsMetricsByTopic <- consumerGroupsMetricsByConsumerGroup.get(consumerGroup).toRight(CacheError.NoConsumerGroupMetricsForConsumerGroup)
    } yield consumerGroupsMetricsByTopic.mapValues { consumerGroupsMetrics =>
      consumerGroupsMetrics.map { case (cgtp, cgm) => (cgtp.topicPartition.partition, cgm) }.toSortedMap
    }
  }

  def getConsumerGroupTopicMetrics(kafkaClusterId: KafkaClusterEntityId, consumerGroup: String, topic: String): CacheResult[SortedMap[Int, ConsumerGroupMetrics]] = {
    for {
      consumerGroupsMetricsByConsumerGroup <- getConsumerGroupMetricsByConsumerGroupAndTopic(kafkaClusterId).toRight(CacheError.NoConsumerGroupMetricsForKafkaCluster)
      consumerGroupsMetricsByTopic <- consumerGroupsMetricsByConsumerGroup.get(consumerGroup).toRight(CacheError.NoConsumerGroupMetricsForConsumerGroup)
      consumerGroupsMetrics <- consumerGroupsMetricsByTopic.get(topic).toRight(CacheError.NoConsumerGroupMetricsForTopic)
    } yield consumerGroupsMetrics.map { case (cgtp, cgm) => (cgtp.topicPartition.partition, cgm) }.toSortedMap
  }
}
