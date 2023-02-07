/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster.observers

import cats.syntax.either._
import com.typesafe.scalalogging.LazyLogging
import com.mwam.kafkakewl.api.metrics.kafkacluster.{KafkaClusterObserver, TopicInfoObserver}
import com.mwam.kafkakewl.domain.kafka.KafkaTopic
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId

import scala.collection.{SortedMap, concurrent}

trait TopicInfoCache {
  def getKafkaClusterIds: CacheResult[Iterable[KafkaClusterEntityId]]
  def getTopics(kafkaClusterId: KafkaClusterEntityId): CacheResult[Iterable[String]]
  def getTopicInfo(kafkaClusterId: KafkaClusterEntityId, topicName: String): CacheResult[KafkaTopic.Info]
}

class TopicInfoCacheImpl extends TopicInfoCache
  with LazyLogging
  with KafkaClusterObserver
  with TopicInfoObserver {

  private val topicInfos = concurrent.TrieMap[KafkaClusterEntityId, SortedMap[String, KafkaTopic.Info]]()

  def remove(kafkaClusterId: KafkaClusterEntityId): Unit = {
    topicInfos.remove(kafkaClusterId)
  }

  def updateAllTopicInfos(kafkaClusterId: KafkaClusterEntityId, allTopicInfos: SortedMap[String, KafkaTopic.Info]): Unit = {
    topicInfos += (kafkaClusterId -> allTopicInfos)
  }

  def getKafkaClusterIds: CacheResult[Iterable[KafkaClusterEntityId]] = topicInfos.keys.toSeq.sortBy(_.id).asRight

  def getTopics(kafkaClusterId: KafkaClusterEntityId): CacheResult[Iterable[String]] = {
    for {
      topicInfos <- topicInfos.get(kafkaClusterId).toRight(CacheError.NoTopicInfosForKafkaCluster)
    } yield topicInfos.keys
  }

  def getTopicInfo(kafkaClusterId: KafkaClusterEntityId, topicName: String): CacheResult[KafkaTopic.Info] = {
    for {
      topicInfos <- topicInfos.get(kafkaClusterId).toRight(CacheError.NoTopicInfosForKafkaCluster)
      topicInfo <- topicInfos.get(topicName).toRight(CacheError.NoTopicInfoForTopic)
    } yield topicInfo
  }
}
