/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.metrics

import akka.actor.ActorSystem
import akka.stream.Materializer
import cats.syntax.either._
import cats.syntax.option._
import com.mwam.kafkakewl.common.http.HttpClientHelper
import com.mwam.kafkakewl.common.topology.TopologyToDeployOperations
import com.mwam.kafkakewl.domain.deploy.DeployedTopology
import com.mwam.kafkakewl.domain.{CommandError, ValueOrCommandError}
import com.mwam.kafkakewl.domain.kafka.KafkaTopic
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.metrics.{AggregatedConsumerGroupStatus, DeployedTopologyMetrics, DeployedTopologyMetricsCompact, TopicPartitionsConsumerGroupMetrics}
import com.mwam.kafkakewl.domain.topology.TopicId
import com.mwam.kafkakewl.utils.MdcUtils
import com.typesafe.scalalogging.LazyLogging

import scala.collection.SortedMap
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration._

class MetricsServiceImpl(
  metricsServiceClient: MetricsServiceClient
)(implicit
  system: ActorSystem,
  ec: ExecutionContextExecutor,
  mat: Materializer
) extends MetricsService
  with HttpClientHelper
  with LazyLogging
  with TopologyToDeployOperations
  with MdcUtils {

  private def createDeployedTopologyMetrics(
    allTopicIdByName: Map[KafkaClusterEntityId, Map[String, TopicId]],
    topicInfosByKafkaClusterId: Map[KafkaClusterEntityId, SortedMap[String, scala.Either[CommandError, KafkaTopic.Info]]],
    consumerGroupMetricsByKafkaClusterId: Map[KafkaClusterEntityId, SortedMap[String, scala.Either[CommandError, TopicPartitionsConsumerGroupMetrics]]]
  )(deployedTopology: DeployedTopology): Option[DeployedTopologyMetrics] = {
    for {
      allTopicInfos <- topicInfosByKafkaClusterId.get(deployedTopology.kafkaClusterId)
      topicIdByName <- allTopicIdByName.get(deployedTopology.kafkaClusterId)
      allConsumerGroupMetrics <- consumerGroupMetricsByKafkaClusterId.get(deployedTopology.kafkaClusterId)
      topologyWithVersion <- deployedTopology.topologyWithVersion
    } yield {
      val topology = topologyWithVersion.topology

      // topic infos, replacing any error or missing topic-info with a None
      val topicInfos = topology.fullyQualifiedTopics.mapValues(topic => allTopicInfos.get(topic.name).flatMap(_.toOption))

      // application infos - at the moment never None, but the consumer-group metrics inside can be
      val applicationInfos = topology.fullyQualifiedApplications.mapValues { application =>
        val consumerGroupMetrics = for {
          consumerGroupId <- application.actualConsumerGroup
          consumerGroupMetrics <- allConsumerGroupMetrics.get(consumerGroupId).flatMap(_.toOption)
        } yield consumerGroupMetrics

        DeployedTopologyMetrics.Application.Info(
          consumerGroupMetrics.map { topicPartitionsConsumerGroupMetrics =>
            val topicPartitionsConsumerGroupMetricsByTopicId = topicPartitionsConsumerGroupMetrics.flatMap {
              case (topicName, partitionsConsumerGroupMetrics) =>
                topicIdByName
                  .get(topicName)
                  .map(tid => (
                    tid,
                    DeployedTopologyMetrics.Application.ConsumerGroup.Topic.Info(
                      partitionsConsumerGroupMetrics.values.combineStatus,
                      partitionsConsumerGroupMetrics.values.combineStatusAggregated,
                      partitionsConsumerGroupMetrics.values.sumLag,
                      partitionsConsumerGroupMetrics,
                      partitionsConsumerGroupMetrics.values.sumConsumptionRate
                    )
                  ))
            }.toMap

            DeployedTopologyMetrics.Application.ConsumerGroup.Info(
              topicPartitionsConsumerGroupMetricsByTopicId.values.map(_.aggregateConsumerGroupStatus).combine,
              topicPartitionsConsumerGroupMetricsByTopicId.values.flatMap(_.aggregatedConsumerGroupStatus).combine,
              topicPartitionsConsumerGroupMetricsByTopicId
            )
          }
        ).some
      }

      val aggregateConsumerGroupStatus = applicationInfos.values.flatten.flatMap(_.consumerGroupStatus).map(_.aggregateConsumerGroupStatus).combine
      val aggregatedConsumerGroupStatus = applicationInfos.values.flatten.flatMap(_.consumerGroupStatus).flatMap(_.aggregatedConsumerGroupStatus).combine

      DeployedTopologyMetrics(
        deployedTopology.kafkaClusterId,
        deployedTopology.topologyId,
        aggregateConsumerGroupStatus,
        aggregatedConsumerGroupStatus,
        topicInfos,
        applicationInfos
      )
    }
  }

  private def createDeployedTopologyMetricsCompact(
    consumerGroupMetricsByKafkaClusterId: Map[KafkaClusterEntityId, SortedMap[String, scala.Either[CommandError, TopicPartitionsConsumerGroupMetrics]]]
  )(deployedTopology: DeployedTopology): Option[DeployedTopologyMetricsCompact] = {
    for {
      allConsumerGroupMetrics <- consumerGroupMetricsByKafkaClusterId.get(deployedTopology.kafkaClusterId)
      topologyWithVersion <- deployedTopology.topologyWithVersion
    } yield {
      val topology = topologyWithVersion.topology

      val consumerGroupStatuses = topology.applications.values
        .flatMap { application =>
          for {
            consumerGroupId <- application.actualConsumerGroup
            consumerGroupMetrics <- allConsumerGroupMetrics.get(consumerGroupId).flatMap(_.toOption)
          } yield consumerGroupMetrics.values.flatMap(_.values).map(_.status)
        }
        .flatten

      DeployedTopologyMetricsCompact(
        deployedTopology.kafkaClusterId,
        deployedTopology.topologyId,
        consumerGroupStatuses.combine,
        consumerGroupStatuses.map(AggregatedConsumerGroupStatus(_)).combine
      )
    }
  }

  def getDeployedTopologiesMetrics(
    allCurrentDeployedTopologies: Iterable[DeployedTopology],
    deployedTopologies: Iterable[DeployedTopology]
  ): ValueOrCommandError[Seq[DeployedTopologyMetrics]] = {
    val allCurrentTopologiesByKafkaClusterId = allCurrentDeployedTopologies
      .groupBy(_.kafkaClusterId)
      .mapValues(_.flatMap(dt => dt.topologyWithVersion.map(tv => tv.topology)))
    val deployedTopologiesByKafkaClusterId = deployedTopologies
      .groupBy(_.kafkaClusterId)
      .mapValues(_.flatMap(dt => dt.topologyWithVersion.map(tv => tv.topology)))

    // paralellize the 2 queries to the metrics service
    val topicInfosByKafkaClusterIdFunc = decorateWithCurrentMDC { () => metricsServiceClient.getTopicInfos(allCurrentTopologiesByKafkaClusterId, deployedTopologiesByKafkaClusterId) }
    val topicInfosByKafkaClusterIdFuture = Future { topicInfosByKafkaClusterIdFunc() }

    val consumerGroupMetricsByKafkaClusterIdFunc = decorateWithCurrentMDC { () => metricsServiceClient.getConsumerGroupMetrics(allCurrentTopologiesByKafkaClusterId, deployedTopologiesByKafkaClusterId) }
    val consumerGroupMetricsByKafkaClusterIdFuture = Future { consumerGroupMetricsByKafkaClusterIdFunc() }

    // build the results from the parallel futures
    val (topicInfosByKafkaClusterId, consumerGroupMetricsByKafkaClusterId) = Await.result(for {
      topicInfosByKafkaClusterId <- topicInfosByKafkaClusterIdFuture
      consumerGroupMetricsByKafkaClusterId <- consumerGroupMetricsByKafkaClusterIdFuture
    } yield (topicInfosByKafkaClusterId, consumerGroupMetricsByKafkaClusterId), Duration.Inf)

    // building this map just once
    val allTopicIdByName = allCurrentTopologiesByKafkaClusterId.mapValues {
      _.flatMap(_.fullyQualifiedTopics.map { case (tid, topic) => (topic.name, tid) }).toMap
    }

    deployedTopologies
      .flatMap(createDeployedTopologyMetrics(allTopicIdByName, topicInfosByKafkaClusterId, consumerGroupMetricsByKafkaClusterId))
      .toSeq
      .asRight
  }

  def getDeployedTopologiesMetricsCompact(
    allCurrentDeployedTopologies: Iterable[DeployedTopology],
    deployedTopologies: Iterable[DeployedTopology]
  ): ValueOrCommandError[Seq[DeployedTopologyMetricsCompact]] = {
    val allCurrentTopologiesByKafkaClusterId = allCurrentDeployedTopologies
      .groupBy(_.kafkaClusterId)
      .mapValues(_.flatMap(dt => dt.topologyWithVersion.map(tv => tv.topology)))
    val deployedTopologiesByKafkaClusterId = deployedTopologies
      .groupBy(_.kafkaClusterId)
      .mapValues(_.flatMap(dt => dt.topologyWithVersion.map(tv => tv.topology)))

    val consumerGroupMetricsByKafkaClusterId = metricsServiceClient.getConsumerGroupMetrics(allCurrentTopologiesByKafkaClusterId, deployedTopologiesByKafkaClusterId)

    deployedTopologies
      .flatMap(createDeployedTopologyMetricsCompact(consumerGroupMetricsByKafkaClusterId))
      .toSeq
      .asRight
  }
}
