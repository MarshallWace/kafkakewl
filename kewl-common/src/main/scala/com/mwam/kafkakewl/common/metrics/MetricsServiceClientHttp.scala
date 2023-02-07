/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.metrics

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest}
import akka.stream.Materializer
import cats.data.EitherT
import cats.syntax.either._
import cats.instances.future._
import io.circe.syntax._
import com.mwam.kafkakewl.utils._
import com.mwam.kafkakewl.common.http.HttpClientHelper
import com.mwam.kafkakewl.common.topology.TopologyToDeployOperations
import com.mwam.kafkakewl.domain.CommandError
import com.mwam.kafkakewl.domain.kafka.KafkaTopic
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.metrics.TopicPartitionsConsumerGroupMetrics
import com.mwam.kafkakewl.domain.metrics.JsonEncodeDecode._
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology.{TopologyEntityId, TopologyToDeploy}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.SortedMap
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration._
import scala.util.Try

class MetricsServiceClientHttp(
  metricsServiceUri: String,
  topicDefaults: TopicDefaults
)(implicit
  system: ActorSystem,
  ec: ExecutionContextExecutor,
  mat: Materializer
) extends MetricsServiceClient
  with HttpClientHelper
  with LazyLogging
  with TopologyToDeployOperations {

  private def getConsumedTopicNamesByConsumerGroup(
    currentTopologies: Map[TopologyEntityId, TopologyToDeploy],
    deployedTopologies: Iterable[TopologyToDeploy]
  ): Map[String, Set[String]] = {
    // assuming the the consumer group - consumed topic pairs are unique
    // (must be the case otherwise 2 applications consuming the same topics with the same consumer group could cause problems)
    deployedTopologies
      .flatMap(topology => topology.fullyQualifiedApplications.toList.flatMap { // toList makes sure that we can keep the duplicate consumerGroups
        // for all applications, gathering the consumer group (if there is any) and the set of topics that it actually consumes via relationships
        // the topics are needed because later we need to filter for the topics' consumer group status which are actually consumed
        case (applicationId, application) =>
          application.actualConsumerGroup.map { consumerGroup =>
            val consumedTopicNames = allRelationshipsOfApplication(currentTopologies, topology.topologyEntityId, topology, applicationId, topicDefaults)
              .collect {
                case r: TopologyToDeploy.ApplicationTopicRelationship if r.monitorConsumerLag =>
                  r.resolveNode2.asTopicOrNone[TopologyToDeploy.Topic].map(t => t.topologyNode.node.name)
              }
              .flatten.toSet
            (consumerGroup, consumedTopicNames)
          }
      })
      // skipping the consumer groups which have no consumed topics
      .filter { case (_, consumedTopicNames) => consumedTopicNames.nonEmpty}
      // aggregating the consumed topics sets for consumer groups (consumer groups are not unique: mainly to support kafka-streams parts as separate applications, I allowed it)
      .groupBy { case (consumerGroup, _) => consumerGroup }
      .mapValues(_.map { case (_, consumerTopicNames) => consumerTopicNames }.reduce(_ ++ _))
  }

  def getTopicInfos(
    allCurrentDeployedTopologiesByKafkaClusterId: Map[KafkaClusterEntityId, Iterable[TopologyToDeploy]],
    deployedTopologiesByKafkaClusterId: Map[KafkaClusterEntityId, Iterable[TopologyToDeploy]]
  ): Map[KafkaClusterEntityId, SortedMap[String, Either[CommandError, KafkaTopic.Info]]] = {
    logger.info(s"getTopicInfos() starting...")
    try
      deployedTopologiesByKafkaClusterId.par.map { case (kid, deployedTopologies) =>
        val topicNames = deployedTopologies.flatMap(_.topics.values.map(_.name))
        val request = HttpRequest(uri = s"$metricsServiceUri/$kid/topic", entity = HttpEntity(ContentTypes.`application/json`, topicNames.asJson.noSpaces))
        val responseResult = for {
          stringResponse <- EitherT.right(toStringResponse(Http().singleRequest(request)))
          topicInfo <- EitherT.fromEither[Future](decodeResponse[SortedMap[String, Either[String, KafkaTopic.Info]]](stringResponse))
        } yield topicInfo
        val topicInfos = Try(Await.result(responseResult.value, 20.seconds)).toEither.leftMap(CommandError.exception).flatMap(x => x) match {
          case Left(e) => topicNames.map(t => (t, Left(e))).toSortedMap
          case Right(r) => r.mapValues(_.leftMap(CommandError.otherError))
        }
        (kid, topicInfos)
      }.seq
    finally logger.info(s"getTopicInfos() finished.")
  }

  def getConsumerGroupMetrics(
    allCurrentDeployedTopologiesByKafkaClusterId: Map[KafkaClusterEntityId, Iterable[TopologyToDeploy]],
    deployedTopologiesByKafkaClusterId: Map[KafkaClusterEntityId, Iterable[TopologyToDeploy]]
  ): Map[KafkaClusterEntityId, SortedMap[String, Either[CommandError, TopicPartitionsConsumerGroupMetrics]]] = {
    logger.info(s"getConsumerGroupMetrics() starting...")
    try
      // intersect currentTopologies and deployedTopologiesByKafkaClusterId by kafka-cluster-id
      deployedTopologiesByKafkaClusterId.keySet.intersect(allCurrentDeployedTopologiesByKafkaClusterId.keySet)
        // these are safe due to the intersecting above
        .map(kid => (kid, (deployedTopologiesByKafkaClusterId(kid), allCurrentDeployedTopologiesByKafkaClusterId(kid))))
        // parallel processing of clusters
        .par.map {
        case (kid, (deployedTopologies, currentTopologies)) =>
          val consumerGroupsWithConsumedTopicNames = getConsumedTopicNamesByConsumerGroup(currentTopologies.map(t => (t.topologyEntityId, t)).toMap, deployedTopologies)
          val consumerGroupIds = consumerGroupsWithConsumedTopicNames.keys

          val request = HttpRequest(uri = s"$metricsServiceUri/$kid/group", entity = HttpEntity(ContentTypes.`application/json`, consumerGroupIds.asJson.noSpaces))
          val responseResult = for {
            stringResponse <- EitherT.right(toStringResponse(Http().singleRequest(request)))
            consumerGroupMetrics <- EitherT.fromEither[Future](decodeResponse[SortedMap[String, Either[String, TopicPartitionsConsumerGroupMetrics]]](stringResponse))
          } yield consumerGroupMetrics
          val consumerGroupsMetrics = Try(Await.result(responseResult.value, 20.seconds)).toEither
            .leftMap(CommandError.exception).flatMap(x => x) match {
            case Left(e) => consumerGroupIds.map(cg => (cg, Left(e))).toSortedMap
            case Right(r) => r.map { case (consumerGroup, topicPartitionsConsumerGroupMetrics) =>
              (
                consumerGroup,
                topicPartitionsConsumerGroupMetrics
                  .leftMap(CommandError.otherError)
                  // filtering the resulting topic-partitions metrics for the topics that this consumer group is actually consumes (has a consume relationship to)
                  .map(_.filter { case (topicName, _) => consumerGroupsWithConsumedTopicNames(consumerGroup)(topicName) })
              )
            }
          }
          (kid, consumerGroupsMetrics)
      }.seq.toMap
    finally logger.info(s"getConsumerGroupMetrics() finished.")
  }
}
