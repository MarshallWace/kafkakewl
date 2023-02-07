/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import com.mwam.kafkakewl.domain.deploy.{OffsetOfTopicPartition, UnsafeKafkaClusterChangeDescription}
import com.mwam.kafkakewl.domain.kafka.{KafkaConsumerGroup, KafkaTopic}
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.topology._

/**
  * The safety-allowed combination of the kafka cluster command.
  */
sealed trait KafkaClusterCommandActionSafety
object KafkaClusterCommandActionSafety {
  final case object UnsafeNotAllowed extends KafkaClusterCommandActionSafety
  final case object UnsafeAllowed extends KafkaClusterCommandActionSafety
  final case object UnsafePartiallyAllowed extends KafkaClusterCommandActionSafety
  final case object Safe extends KafkaClusterCommandActionSafety
}

/**
  * The possible kafka cluster item entity types.
  */
sealed trait KafkaClusterItemEntityType
object KafkaClusterItemEntityType {
  final case object Topic extends KafkaClusterItemEntityType
  final case object Acl extends KafkaClusterItemEntityType
}

/**
  * The execution details of a kafka cluster command.
  */
final case class KafkaClusterCommandActionExecution(action: String, success: Boolean, result: String)

/**
  * Describes a single action and its result that was attempted on the kafka cluster (e.g. create new topic, delete topic, update topic config, etc...)
  */
final case class KafkaClusterDeployCommandResponseAction(
  action: String,
  safety: KafkaClusterCommandActionSafety,
  notAllowedUnsafeChange: Option[UnsafeKafkaClusterChangeDescription],
  execution: Option[KafkaClusterCommandActionExecution] = None
)

sealed trait KafkaClusterCommandResponse
object KafkaClusterCommandResponse {
  final case class Deployment(deployActions: Seq[KafkaClusterDeployCommandResponseAction] = Seq.empty, deployedTopologyChanged: Boolean) extends KafkaClusterCommandResponse

  final case class DiffResult(topics: DiffResult.Topics, acls: DiffResult.Acls) extends KafkaClusterCommandResponse
  object DiffResult {
    final case class Topics(missingFromKafka: Seq[String], notNeededInKafka: Seq[String], differences: Seq[Topics.Difference])
    object Topics {
      final case class Difference(kafka: String, topology: String)
    }
    final case class Acl(perm: String, user: String, resource: String, details: String)
    final case class Acls(missingFromKafka: Seq[Acl], notNeededInKafka: Seq[Acl]) extends KafkaClusterCommandResponse
  }

  final case class ApplicationOffsetsReset(
    consumerGroup: String,
    topicPartitionsResetToBeginning: Seq[OffsetOfTopicPartition] = Seq.empty,
    topicPartitionsResetToEnd: Seq[OffsetOfTopicPartition] = Seq.empty,
    topicPartitionsResetToOther: Seq[OffsetOfTopicPartition] = Seq.empty,
    deletedTopics: Seq[String] = Seq.empty
  ) extends KafkaClusterCommandResponse

  final case class Connector(connector: String, keys: Seq[String]) extends KafkaClusterCommandResponse
  final case class ConnectReplicator(connectReplicator: String, keys: Seq[String]) extends KafkaClusterCommandResponse

  final case class TopicIds(ids: Seq[LocalTopicId]) extends KafkaClusterCommandResponse
  final case class ApplicationIds(ids: Seq[LocalApplicationId]) extends KafkaClusterCommandResponse

  final case class DeployedTopic(
    topicId: LocalTopicId,
    topic: TopologyToDeploy.Topic,
    kafkaTopicInfo: KafkaTopic.Info
  ) extends KafkaClusterCommandResponse

  final case class DeployedApplication(
    applicationId: LocalApplicationId,
    application: TopologyToDeploy.Application,
    consumerGroupInfo: Option[KafkaConsumerGroup.Info]
  ) extends KafkaClusterCommandResponse
}

/**
  * The possible results of a kafka cluster command.
  */
sealed trait KafkaClusterCommandResult extends CommandResultBase {
  val kafkaClusterId: KafkaClusterEntityId
}
object KafkaClusterCommandResult {
  final case class Failed(
    metadata: CommandMetadata,
    kafkaClusterId: KafkaClusterEntityId,
    reasons: Seq[CommandError] = Seq.empty,
    // even a failure can have some response of the partial results (e.g. deployments)
    response: Option[KafkaClusterCommandResponse] = None
  ) extends KafkaClusterCommandResult with CommandResultBase.Failed

  final case class Succeeded(
    metadata: CommandMetadata,
    kafkaClusterId: KafkaClusterEntityId,
    response: KafkaClusterCommandResponse
  ) extends KafkaClusterCommandResult
}
