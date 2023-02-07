/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.metrics


import com.github.blemale.scaffeine.Scaffeine
import com.mwam.kafkakewl.domain.CommandError
import com.mwam.kafkakewl.domain.kafka.KafkaTopic
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.metrics.TopicPartitionsConsumerGroupMetrics
import com.mwam.kafkakewl.domain.topology.TopologyToDeploy
import com.mwam.kafkakewl.utils.MdcUtils

import scala.collection.SortedMap
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * A caching client for the metrics service.
  *
  * It always retrieves topic and consumer group infos for ALL deployed topologies, not just the ones requested and keeps them in a cache in for the expiry time.
  *
  * The only reason of using Caffeine is to be able to deal with many concurrent requests with a future. Not a big deal, but easier than doing it manually.
  *
  * @param metricsServiceClient the underlying metrics service client
  * @param expiry the optional cache expiry, by default 3 seconds.
  */
class MetricsServiceClientCaching(
  metricsServiceClient: MetricsServiceClient,
  expiry: Duration = 3.seconds
) extends MetricsServiceClient with MdcUtils {

  private type TopicInfos = Map[KafkaClusterEntityId, SortedMap[String, Either[CommandError, KafkaTopic.Info]]]
  private type ConsumerGroupMetrics = Map[KafkaClusterEntityId, SortedMap[String, Either[CommandError, TopicPartitionsConsumerGroupMetrics]]]

  private val cachedTopicInfos = Scaffeine()
    // it's really just a single cached item
    .expireAfterWrite(expiry).maximumSize(1)
    .buildAsync[Unit, TopicInfos]()

  private val cachedConsumerGroupMetrics = Scaffeine()
    // it's really just a single cached item
    .expireAfterWrite(expiry).maximumSize(1)
    .buildAsync[Unit, ConsumerGroupMetrics]()

  def getTopicInfos(
    allCurrentDeployedTopologiesByKafkaClusterId: Map[KafkaClusterEntityId, Iterable[TopologyToDeploy]],
    deployedTopologiesByKafkaClusterId: Map[KafkaClusterEntityId, Iterable[TopologyToDeploy]]
  ): Map[KafkaClusterEntityId, SortedMap[String, Either[CommandError, KafkaTopic.Info]]] = {
    val topicInfosFuture = cachedTopicInfos.get(
      (),
      // if the cached topic-infos is expired or empty then get ALL deployed topologies' topic infos from the metrics service
      decorateWithCurrentMDC {
        _ => metricsServiceClient.getTopicInfos(allCurrentDeployedTopologiesByKafkaClusterId, allCurrentDeployedTopologiesByKafkaClusterId)
      }
    )
    Await.result(topicInfosFuture, Duration.Inf)
  }

  def getConsumerGroupMetrics(
    allCurrentDeployedTopologiesByKafkaClusterId: Map[KafkaClusterEntityId, Iterable[TopologyToDeploy]],
    deployedTopologiesByKafkaClusterId: Map[KafkaClusterEntityId, Iterable[TopologyToDeploy]]
  ): Map[KafkaClusterEntityId, SortedMap[String, Either[CommandError, TopicPartitionsConsumerGroupMetrics]]] = {
    val consumerGroupMetricsFuture = cachedConsumerGroupMetrics.get(
      (),
      // if the cached consumer-group-infos is expired or empty then get ALL deployed topologies' consumer group infos from the metrics service
      decorateWithCurrentMDC {
        _ => metricsServiceClient.getConsumerGroupMetrics(allCurrentDeployedTopologiesByKafkaClusterId, allCurrentDeployedTopologiesByKafkaClusterId)
      }
    )
    Await.result(consumerGroupMetricsFuture, Duration.Inf)
  }
}
