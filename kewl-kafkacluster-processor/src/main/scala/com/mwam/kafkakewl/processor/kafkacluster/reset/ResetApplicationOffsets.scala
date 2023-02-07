/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.reset

import cats.syntax.either._
import com.mwam.kafkakewl.common.validation.Validation
import com.mwam.kafkakewl.common.validation.Validation._
import com.mwam.kafkakewl.domain.{CommandError, KafkaClusterCommand, KafkaClusterCommandResult}
import com.mwam.kafkakewl.kafka.utils.KafkaExtensions._
import com.mwam.kafkakewl.common.topology.TopologyToDeployOperations
import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.domain.deploy.DeployedTopology.ResetApplicationOptions
import com.mwam.kafkakewl.domain.deploy.{OffsetOfTopicPartition, TopicPartitionPosition, TopicPartitionPositionOfTopicPartition}
import com.mwam.kafkakewl.domain.kafkacluster.KafkaCluster
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology._
import com.mwam.kafkakewl.processor.kafkacluster.common.{KafkaAdminClientExtraSyntax, KafkaConnectionOperations, KafkaConsumerExtraSyntax}

trait ResetApplicationOffsetsCommon {
  this: ResetCommon with TopologyToDeployOperations =>

  val topicDefaults: TopicDefaults

  def isInternalTopic(kafkaStreamsApplicationId: String, topic: String): Boolean =
    topic.startsWith(kafkaStreamsApplicationId + "-") && (topic.endsWith("-changelog") || topic.endsWith("-repartition"))

  def validateTopicPartitionsInTopology(
    topologyToDeploy: TopologyToDeploy,
    topicPartitions: Iterable[(TopicId, Option[Int])]
  ): Result = {
    val partitionsByTopic = topologyToDeploy.fullyQualifiedTopics.mapValues(_.partitions)

    topicPartitions
      .map {
        case (topicId, partitionOrNone) =>
          partitionsByTopic
            .get(topicId)
            .map(numberOfPartitions =>
              partitionOrNone.map { partition =>
                if (partition >= numberOfPartitions) Validation.Result.validationError(s"Topic ${topicId.quote} partition = $partition is invalid (number of partitions = $numberOfPartitions)")
                else if (partition < 0) Validation.Result.validationError(s"Topic ${topicId.quote} partition = $partition is invalid")
                else Validation.Result.success
              }
                .getOrElse(Validation.Result.success)
            )
            .getOrElse(Validation.Result.validationError(s"topic ${topicId.quote} not in the topology"))
      }
      .combine()
  }

  def calculateTopicPartitionsToReset(
    currentTopologies: Map[TopologyEntityId, TopologyToDeploy],
    topologyId: TopologyEntityId,
    applicationId: ApplicationId,
    options: ResetApplicationOptions.ApplicationTopicOffsetsOptions
  ): Map[String, Iterable[(Int, TopicPartitionPosition)]] = {
    currentTopologies
      .get(topologyId)
      .map(allRelationshipsOfApplication(currentTopologies, topologyId, _, applicationId, topicDefaults))
      .getOrElse(Iterable.empty)
      .collect {
        // for consume-relationships only
        case r: TopologyToDeploy.ApplicationTopicRelationship if r.relationship.isConsume =>
          val topologyNamespace = r.resolveNode1.topologyNode.topologyNamespace
          val topicId = r.resolveNode2.topologyNode.nodeId
          val topic = r.resolveNode2.topologyNode.node
          val resetMode = r.properties.reset.getOrElse(ResetMode.Beginning)
          // all partitions of the topic
          (0 until topic.partitions)
            .flatMap(partition =>
              // extracting the optional position for this topic-partition (if None, we skip it, no need to reset)
              for {
                position <- options.topicPartitionPositionFor(topologyNamespace, topicId, partition)
                topicPartitionPosition <- position match {
                  // if the position is default, we try to extract it from the relationship (may return None in which case again we do not reset)
                  case TopicPartitionPosition.Default() => TopicPartitionPosition.fromResetMode(resetMode).map(p => (topic.name, (partition, p)))
                  case _ => Some(topic.name, (partition, position))
                }
              } yield topicPartitionPosition
            )
      }
      .flatten
      .groupBy(_._1)
      // create the map by topic-name
      .map { case (k, ps) => (k, ps.map(_._2).toSeq.sortBy(_._1)) }
  }

  def calculateTopicsToBeDeleted(
    topicNamesOrError: => ValueOrCommandErrors[IndexedSeq[String]], // by-name so that it's used only for kafka-streams apps
    currentTopologies: Map[TopologyEntityId, TopologyToDeploy],
    topologyId: TopologyEntityId,
    applicationId: ApplicationId,
    options: ResetApplicationOptions.ApplicationTopicOffsetsOptions
  ): ValueOrCommandErrors[IndexedSeq[String]] = {
    val kafkaStreamsAppIdOrNone = for {
      topology <- currentTopologies.get(topologyId)
      application <- topology.fullyQualifiedApplications.get(applicationId)
      kafkaStreamsApplication <- application.typeAsKafkaStreams
    } yield kafkaStreamsApplication.kafkaStreamsAppId

    kafkaStreamsAppIdOrNone
      .map { kafkaStreamsAppId =>
        for {
          // internal topics of this kafka-streams application
          allTopics <- topicNamesOrError
          internalTopics = allTopics.filter(isInternalTopic(kafkaStreamsAppId, _))
        } yield internalTopics.filter(t => options.deleteKafkaStreamsInternalTopics.exists(_.doesMatch(t)))
      }
      .getOrElse(Right(IndexedSeq.empty))
  }
}

trait ResetApplicationOffsets extends ResetApplicationOffsetsCommon {
  this: ResetCommon
    with KafkaConnectionOperations
    with KafkaConsumerExtraSyntax
    with KafkaAdminClientExtraSyntax
    with TopologyToDeployOperations =>

  def resetApplicationOffsets(
    kafkaCluster: KafkaCluster,
    command: KafkaClusterCommand.DeployedTopologyReset,
    currentTopologies: Map[TopologyEntityId, TopologyToDeploy],
    topologyId: TopologyEntityId,
    topologyToDeploy: TopologyToDeploy,
    applicationId: ApplicationId,
    options: ResetApplicationOptions.ApplicationTopicOffsetsOptions
  ): Either[KafkaClusterCommandResult.Failed, KafkaClusterCommandResult.Succeeded] = {

    def topicPartitionOffsetIsIn(tpo: OffsetOfTopicPartition, s: Set[(String, Int)]): Boolean =
      s((tpo.topic, tpo.partition))
    def topicPartitionOffsetNotIn(tpo: OffsetOfTopicPartition, s1: Set[(String, Int)], s2: Set[(String, Int)]): Boolean =
      !s1((tpo.topic, tpo.partition)) && !s2((tpo.topic, tpo.partition))

    val result = for {
      // basic validation
      _ <- Seq(
        validateApplication(topologyToDeploy, applicationId),
        validateTopicPartitionsInTopology(topologyToDeploy, options.expectExistingTopicPartitions(topologyToDeploy.topologyNamespace)),
        Validation.Result.validationErrorIf(
          topologyToDeploy.fullyQualifiedApplications.get(applicationId).flatMap(_.actualConsumerGroup).isEmpty,
          s"application '$applicationId' does not have a consumer group"
        )
      ).combine().toCommandErrors

      consumerGroupId = topologyToDeploy.fullyQualifiedApplications(applicationId).actualConsumerGroup.get

      topicPartitionsToReset = calculateTopicPartitionsToReset(currentTopologies, topologyId, applicationId, options)
      topicPartitionPositions = topicPartitionsToReset.flatMap {
        case (topicName, partitionPositions) =>
          partitionPositions.map { case (partition, position) => TopicPartitionPositionOfTopicPartition(topicName, partition, position) }
      }

      topicPartitionSetResetToBeginning = topicPartitionPositions
        .collect { case tpp if tpp.position == TopicPartitionPosition.Beginning() => (tpp.topic, tpp.partition) }.toSet
      topicPartitionSetResetToEnd = topicPartitionPositions
        .collect { case tpp if tpp.position == TopicPartitionPosition.End() => (tpp.topic, tpp.partition) }.toSet

      result <- withAdminClientAndCommandErrors { adminClient =>
        for {
          // fail-fast if the consumer group is active
          _ <- adminClient.ensureConsumerGroupIsNotLive(consumerGroupId)

          topicsToBeDeleted <- calculateTopicsToBeDeleted(
            adminClient.topicNames().toRightOrCommandErrors,
            currentTopologies,
            topologyId,
            applicationId,
            options
          )

          // removing un-managed topics that don't exist (they can be safely ignored)
          nonExistingUnmanagedTopicNames = topicPartitionPositions
            .filter(tpp => topologyToDeploy.fullyQualifiedTopics.values.find(_.name == tpp.topic).exists(_.unManaged))
            .toSeq.map(_.topic).distinct
            .filter(topicName => !adminClient.topicExists(topicName).getOrElse(false))
            .toSet
          filteredTopicPartitionPositions = topicPartitionPositions.filter(tpp => !nonExistingUnmanagedTopicNames(tpp.topic))

          // checking for authorization code if we're about to perform any change
          _ <- failIfAuthorizationCodeNeeded(
            kafkaCluster,
            topologyToDeploy.deployWithAuthorizationCode,
            // this gets encoded in the authorization code, so that it can't be used for a different operation
            resetSummary = (filteredTopicPartitionPositions.map(_.toString).toSeq.sorted ++ topicsToBeDeleted.sorted).mkString(","),
            command,
            filteredTopicPartitionPositions.nonEmpty || topicsToBeDeleted.nonEmpty,
            command.options.authorizationCode
          )

          // resetting the offsets (or just dry-run)
          topicPartitionOffsets <- withConsumerAndCommandErrors(Some(consumerGroupId)) { consumer =>
            consumer.resetConsumerGroupOffsets(filteredTopicPartitionPositions, command.dryRun)
          }

          // deleting topics (or not, if dry-run)
          _ <-
            if (command.dryRun) ().asRight[Seq[CommandError]]
            else failIfAnyErrors(
              topicsToBeDeleted
                .map(adminClient.deleteTopicIfExists(_).toRightOrCommandErrors)
                .collect { case Left(ce) => ce }
                .flatten
            )
        } yield KafkaClusterCommandResponse.ApplicationOffsetsReset(
          consumerGroupId,
          topicPartitionsResetToBeginning = topicPartitionOffsets.filter(topicPartitionOffsetIsIn(_, topicPartitionSetResetToBeginning)),
          topicPartitionsResetToEnd = topicPartitionOffsets.filter(topicPartitionOffsetIsIn(_, topicPartitionSetResetToEnd)),
          topicPartitionsResetToOther = topicPartitionOffsets.filter(topicPartitionOffsetNotIn(_, topicPartitionSetResetToBeginning, topicPartitionSetResetToEnd)),
          topicsToBeDeleted
        )
      }
    } yield result

    result
      .map(command.succeededResult)
      .leftMap(command.failedResult)
  }
}
