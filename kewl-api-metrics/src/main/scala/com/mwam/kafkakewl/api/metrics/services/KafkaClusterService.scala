/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.services

import com.mwam.kafkakewl.api.metrics.kafkacluster.observers.{ConsumerGroupMetricsCache, TopicInfoCache, TopicMetricsCache}
import com.mwam.kafkakewl.api.metrics.utils.OperationLogging
import com.mwam.kafkakewl.domain.Mdc
import com.mwam.kafkakewl.utils._
import com.mwam.kafkakewl.domain.kafka.KafkaTopic
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.metrics.{ConsumerGroupMetrics, TopicPartitionsConsumerGroupMetrics}
import com.typesafe.scalalogging.LazyLogging

import java.util.UUID
import scala.collection.SortedMap

object KafkaClusterService {
  type Result[T] = Either[String, T]
}

trait KafkaClusterService {
  def getKafkaClusterIds: KafkaClusterService.Result[Iterable[KafkaClusterEntityId]]
  def getTopics(kafkaClusterId: KafkaClusterEntityId): KafkaClusterService.Result[Iterable[String]]
  def getTopicInfo(kafkaClusterId: KafkaClusterEntityId, topic: String): KafkaClusterService.Result[KafkaTopic.Info]
  def getTopicsInfo(kafkaClusterId: KafkaClusterEntityId, topicNames: Iterable[String]): SortedMap[String, KafkaClusterService.Result[KafkaTopic.Info]]
  def getConsumerGroups(kafkaClusterId: KafkaClusterEntityId): KafkaClusterService.Result[Iterable[String]]
  def getConsumerGroupMetrics(kafkaClusterId: KafkaClusterEntityId, consumerGroup: String): KafkaClusterService.Result[TopicPartitionsConsumerGroupMetrics]
  def getConsumerGroupsMetrics(kafkaClusterId: KafkaClusterEntityId, consumerGroups: Iterable[String]): SortedMap[String, KafkaClusterService.Result[TopicPartitionsConsumerGroupMetrics]]
  def getConsumerGroupTopicMetrics(kafkaClusterId: KafkaClusterEntityId, consumerGroup: String, topic: String): KafkaClusterService.Result[SortedMap[Int, ConsumerGroupMetrics]]
}

class KafkaClusterServiceImpl(
  topicInfoStateCache: TopicInfoCache,
  topicMetricsCache: TopicMetricsCache,
  consumerGroupMetricsCache: ConsumerGroupMetricsCache
) extends KafkaClusterService with LazyLogging with OperationLogging with MdcUtils {

  def newCorrelationId(): String = UUID.randomUUID().toString.replace("-", "")
  def correlationIdMdcKeyValue(): (String, String) = Mdc.Keys.correlationId -> newCorrelationId()
  def requestMdcKeyValue(request: String): (String, String) = Mdc.Keys.request -> request
  def mdc(request: String): Map[String, String] = Map(correlationIdMdcKeyValue(), requestMdcKeyValue(request))
  def mdc(request: String, kafkaClusterId: KafkaClusterEntityId): Map[String, String] = Mdc.fromKafkaClusterId(kafkaClusterId) + correlationIdMdcKeyValue + requestMdcKeyValue(request)

  def getKafkaClusterIds: KafkaClusterService.Result[Iterable[KafkaClusterEntityId]] = {
    val request = "getKafkaClusterIds"
    withMDC(mdc(request)) {
      withLoggingMultiple(s"$request()", "kafka-cluster", limit = 10) {
        topicInfoStateCache.getKafkaClusterIds.toResult
      }
    }
  }

  def getTopics(kafkaClusterId: KafkaClusterEntityId): KafkaClusterService.Result[Iterable[String]] = {
    val request = "getTopics"
    withMDC(mdc(request, kafkaClusterId)) {
      withLoggingMultiple(s"$request($kafkaClusterId)", "topic") {
        topicInfoStateCache.getTopics(kafkaClusterId).toResult
      }
    }
  }

  def getTopicInfo(kafkaClusterId: KafkaClusterEntityId, topic: String): KafkaClusterService.Result[KafkaTopic.Info] = {
    val request = "getTopicInfo"
    withMDC(mdc(request, kafkaClusterId)) {
      withLogging(s"$request($kafkaClusterId, $topic)") {
        for {
          topicInfo <- topicInfoStateCache.getTopicInfo(kafkaClusterId, topic).toResult
          metrics <- topicMetricsCache.getTopicMetrics(kafkaClusterId, topic).toResult
        } yield topicInfo.withMetrics(metrics.topicMetrics, metrics.partitionMetrics)
      }
    }
  }

  def getTopicsInfo(kafkaClusterId: KafkaClusterEntityId, topicNames: Iterable[String]): SortedMap[String, KafkaClusterService.Result[KafkaTopic.Info]] = {
    val request = "getTopicsInfo"
    withMDC(mdc(request, kafkaClusterId)) {
      // We don't log the input/output, because they are too big usually, only their sizes
      withLoggingMultipleKeyed(s"$request($kafkaClusterId, ${numberOfItems("topic", topicNames)})", "topic-info") {
        topicNames
          .map { topic =>
            (
              topic,
              for {
                topicInfo <- topicInfoStateCache.getTopicInfo(kafkaClusterId, topic).toResult
                metrics <- topicMetricsCache.getTopicMetrics(kafkaClusterId, topic).toResult
              } yield topicInfo.withMetrics(metrics.topicMetrics, metrics.partitionMetrics)
            )
          }
          .toSortedMap
      }
    }
  }

  def getConsumerGroups(kafkaClusterId: KafkaClusterEntityId): KafkaClusterService.Result[Iterable[String]] = {
    val request = "getConsumerGroups"
    withMDC(mdc(request, kafkaClusterId)) {
      withLoggingMultiple(s"$request($kafkaClusterId)", "consumer-group") {
        consumerGroupMetricsCache.getConsumerGroups(kafkaClusterId).toResult
      }
    }
  }

  def getConsumerGroupMetrics(kafkaClusterId: KafkaClusterEntityId, consumerGroup: String): KafkaClusterService.Result[TopicPartitionsConsumerGroupMetrics] = {
    val request = "getConsumerGroupMetrics"
    withMDC(mdc(request, kafkaClusterId)) {
      withLogging(s"$request($kafkaClusterId, $consumerGroup)") {
        consumerGroupMetricsCache.getConsumerGroupMetrics(kafkaClusterId, consumerGroup).toResult
      }
    }
  }

  def getConsumerGroupsMetrics(kafkaClusterId: KafkaClusterEntityId, consumerGroups: Iterable[String]): SortedMap[String, KafkaClusterService.Result[TopicPartitionsConsumerGroupMetrics]] = {
    val request = "getConsumerGroupsMetrics"
    withMDC(mdc(request, kafkaClusterId)) {
      // We don't log the input/output, because they are too big usually, only their sizes
      withLoggingMultipleKeyed(s"$request($kafkaClusterId, ${numberOfItems("consumer-group", consumerGroups)})", "consumer-group-metric") {
        consumerGroups.map(cg => (cg, consumerGroupMetricsCache.getConsumerGroupMetrics(kafkaClusterId, cg).toResult)).toSortedMap
      }
    }
  }

  def getConsumerGroupTopicMetrics(kafkaClusterId: KafkaClusterEntityId, consumerGroup: String, topic: String): KafkaClusterService.Result[SortedMap[Int, ConsumerGroupMetrics]] = {
    val request = "getConsumerGroupTopicMetrics"
    withMDC(mdc(request, kafkaClusterId)) {
      withLogging(s"$request($kafkaClusterId, $consumerGroup, $topic)") {
        consumerGroupMetricsCache.getConsumerGroupTopicMetrics(kafkaClusterId, consumerGroup, topic).toResult
      }
    }
  }
}
