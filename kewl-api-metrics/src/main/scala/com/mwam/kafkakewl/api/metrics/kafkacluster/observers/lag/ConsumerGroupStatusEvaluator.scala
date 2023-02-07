/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster.observers.lag

import akka.actor.ActorSystem
import com.mwam.kafkakewl.api.metrics.kafkacluster._
import com.mwam.kafkakewl.domain.deploy.{DeployedTopology, DeployedTopologyEntityId}
import com.mwam.kafkakewl.domain.kafka.KafkaTopic
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.metrics.ConsumerGroupOffset
import com.mwam.kafkakewl.kafka.utils.KafkaConnection
import com.typesafe.scalalogging.LazyLogging

import scala.collection.{SortedMap, concurrent}

class ConsumerGroupStatusEvaluator(
  consumerGroupMetricsObserver: ConsumerGroupMetricsObserver,
  configFunc: KafkaClusterEntityId => ConsumerGroupStatusEvaluatorOfKafkaCluster.Config
)(implicit system: ActorSystem) extends LazyLogging
  with KafkaClusterObserver
  with TopicInfoObserver
  with ConsumerGroupOffsetObserver {

  private val consumerGroupStatusCacheByKafkaClusterId = concurrent.TrieMap.empty[KafkaClusterEntityId, ConsumerGroupStatusEvaluatorOfKafkaCluster]
  private val kafkaConnections = concurrent.TrieMap.empty[KafkaClusterEntityId, KafkaConnection]

  private def getOrAdd(kafkaClusterId: KafkaClusterEntityId): ConsumerGroupStatusEvaluatorOfKafkaCluster =
    consumerGroupStatusCacheByKafkaClusterId.getOrElseUpdate(
      kafkaClusterId,
      {
        val evaluator = new ConsumerGroupStatusEvaluatorOfKafkaCluster(kafkaClusterId, consumerGroupMetricsObserver, configFunc(kafkaClusterId))
        kafkaConnections.get(kafkaClusterId).foreach(evaluator.setKafkaConnection)
        evaluator
      }
    )

  override def remove(kafkaClusterId: KafkaClusterEntityId): Unit =
    consumerGroupStatusCacheByKafkaClusterId.remove(kafkaClusterId)

  override def updateAllTopicInfos(kafkaClusterId: KafkaClusterEntityId, allTopicInfos: SortedMap[String, KafkaTopic.Info]): Unit =
    getOrAdd(kafkaClusterId).updateAllTopicInfos(allTopicInfos)

  override def updateAllConsumerGroupOffsets(kafkaClusterId: KafkaClusterEntityId, offsets: Map[ConsumerGroupTopicPartition, ConsumerGroupOffset]): Unit =
    getOrAdd(kafkaClusterId).updateAllConsumerGroupOffsets(offsets)

  override def updateConsumerGroupOffsets(kafkaClusterId: KafkaClusterEntityId, offsets: Map[ConsumerGroupTopicPartition, ConsumerGroupOffset], removedOffsets: Iterable[ConsumerGroupTopicPartition]): Unit =
    getOrAdd(kafkaClusterId).updateConsumerGroupOffsets(offsets, removedOffsets)

  def createOrUpdateDeployedTopology(deployedTopologyId: DeployedTopologyEntityId, deployedTopology: DeployedTopology): Unit =
    getOrAdd(deployedTopology.kafkaClusterId).createOrUpdateDeployedTopology(deployedTopologyId, deployedTopology)

  def removeDeployedTopology(deployedTopologyId: DeployedTopologyEntityId, deployedTopology: DeployedTopology): Unit =
    getOrAdd(deployedTopology.kafkaClusterId).removeDeployedTopology(deployedTopologyId, deployedTopology)

  def kafkaClusterCreated(kafkaClusterId: KafkaClusterEntityId, kafkaConnection: KafkaConnection): Unit = {
    kafkaConnections.put(kafkaClusterId, kafkaConnection)
    getOrAdd(kafkaClusterId).setKafkaConnection(kafkaConnection) // possibly we call setKafkaConnection twice, but that's OK
  }

  def kafkaClusterUpdated(kafkaClusterId: KafkaClusterEntityId, kafkaConnection: KafkaConnection): Unit = {
    kafkaConnections.put(kafkaClusterId, kafkaConnection)
    getOrAdd(kafkaClusterId).setKafkaConnection(kafkaConnection) // possibly we call setKafkaConnection twice, but that's OK
  }

  def kafkaClusterDeleted(kafkaClusterId: KafkaClusterEntityId): Unit = {
    kafkaConnections.remove(kafkaClusterId)
    getOrAdd(kafkaClusterId).removeKafkaConnection()
  }
}
