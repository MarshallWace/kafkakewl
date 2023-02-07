/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster

import com.mwam.kafkakewl.domain.kafka.KafkaTopic
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId

import scala.collection.SortedMap

trait TopicInfoObserver {
  def updateAllTopicInfos(kafkaClusterId: KafkaClusterEntityId, allTopicInfos: SortedMap[String, KafkaTopic.Info]): Unit
}

object TopicInfoObserver {
  def composite(observers: TopicInfoObserver*): TopicInfoObserver =
    (kafkaClusterId: KafkaClusterEntityId, allTopicInfos: SortedMap[String, KafkaTopic.Info]) => observers.foreach(_.updateAllTopicInfos(kafkaClusterId, allTopicInfos))
}
