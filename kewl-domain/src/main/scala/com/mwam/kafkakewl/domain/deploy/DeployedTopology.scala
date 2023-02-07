/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package deploy

import com.mwam.kafkakewl.domain.kafkacluster.{KafkaClusterAndTopology, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.topology._

/**
  * The deployed-topology entity identifier (must be unique globally).
  *
  * @param id the deployed-topology entity identifier
  */
final case class DeployedTopologyEntityId(id: String) extends AnyVal with EntityId

final case class TopologyToDeployCompactWithVersion(
  version: Int,
  topology: TopologyToDeployCompact
)

final case class DeployedTopologyUnsafeOperationCompact(action: String, result: Option[String])
final case class DeployedTopologyFailedOperationCompact(action: String, result: String)

final case class DeployedTopologyCompact(
  kafkaClusterId: KafkaClusterEntityId,
  topologyId: TopologyEntityId,
  deploymentVersion: Int,
  topologyWithVersion: Option[TopologyToDeployCompactWithVersion],
  allActionsSuccessful: Boolean,
  unsafeOperations: Seq[DeployedTopologyUnsafeOperationCompact],
  failedOperations: Seq[DeployedTopologyFailedOperationCompact],
  tags: Seq[String] = Seq.empty,
  labels: Map[String, String] = Map.empty
) extends Entity with Labelled

final case class TopologyToDeployWithVersion(
  version: Int,
  topology: TopologyToDeploy
)
object TopologyToDeployWithVersion {
  def compact(tv: TopologyToDeployWithVersion) = TopologyToDeployCompactWithVersion(tv.version, TopologyToDeploy.compact(tv.topology))
}

final case class DeployedTopology(
  kafkaClusterId: KafkaClusterEntityId,
  topologyId: TopologyEntityId,
  deploymentVersion: Int,
  topologyWithVersion: Option[TopologyToDeployWithVersion],
  allActionsSuccessful: Boolean,
  notRemovedKeys: Seq[String] = Seq.empty,
  kafkaClusterActions: Seq[KafkaClusterDeployCommandResponseAction] = Seq.empty,
  tags: Seq[String] = Seq.empty,
  labels: Map[String, String] = Map.empty
) extends Entity with Labelled

object DeployedTopology {
  def parseId(id: DeployedTopologyEntityId): (KafkaClusterEntityId, TopologyEntityId) = KafkaClusterAndTopology.parseId(id.id)

  def compact(dt: DeployedTopology) =
    DeployedTopologyCompact(
      dt.kafkaClusterId,
      dt.topologyId,
      dt.deploymentVersion,
      dt.topologyWithVersion.map(TopologyToDeployWithVersion.compact),
      dt.allActionsSuccessful,
      dt.kafkaClusterActions.collect {
        case KafkaClusterDeployCommandResponseAction(action, KafkaClusterCommandActionSafety.UnsafeNotAllowed | KafkaClusterCommandActionSafety.UnsafePartiallyAllowed, Some(_), execution) =>
          DeployedTopologyUnsafeOperationCompact(action, execution.map(_.result.take(600)))
      },
      dt.kafkaClusterActions.collect {
        case KafkaClusterDeployCommandResponseAction(_, _, _, Some(exec)) if !exec.success => DeployedTopologyFailedOperationCompact(exec.action, exec.result.take(600))
      },
      dt.tags,
      dt.labels
    )

  implicit class DeployedTopologyExtensions(deployedTopology: DeployedTopology) {
    def id: DeployedTopologyEntityId = DeployedTopologyEntityId(KafkaClusterAndTopology.id(deployedTopology.kafkaClusterId, deployedTopology.topologyId))
    def toLiveState(version: Int, createdBy: String): EntityState.Live[DeployedTopology] =
      EntityState.Live[DeployedTopology](EntityStateMetadata(id.id, version, createdBy), deployedTopology)

    def topologyOrCommandError: ValueOrCommandError[TopologyToDeploy] =
      deployedTopology.topologyWithVersion
        .toRight(CommandError.validationError(s"topology ${deployedTopology.topologyId.quote} is not deployed"))
        .map(_.topology)

    def topologyOrCommandErrors: ValueOrCommandErrors[TopologyToDeploy] = topologyOrCommandError.left.map(Seq(_))
  }

  /**
    * The possible reset application options.
    */
  sealed trait ResetApplicationOptions
  object ResetApplicationOptions {
    final case class Connector(
      keyRegex: Option[String] = None
    ) extends ResetApplicationOptions

    final case class ConnectReplicator(
      topic: Option[String] = None,
      partition: Option[Int] = None
    ) extends ResetApplicationOptions

    sealed trait ApplicationTopicOffsetsOptions {
      def topicPartitionPositionFor(topologyNamespace: Namespace, topicId: TopicId, partition: Int): Option[TopicPartitionPosition]
      def expectExistingTopicPartitions(topologyNamespace: Namespace): Iterable[(TopicId, Option[Int])]
      val deleteKafkaStreamsInternalTopics: Iterable[FlexibleName]
    }

    final case class ApplicationTopics(
      topics: Seq[FlexibleTopologyTopicId] = Seq(FlexibleTopologyTopicId.Any()),
      position: TopicPartitionPosition = TopicPartitionPosition.Default(),
      deleteKafkaStreamsInternalTopics: Iterable[FlexibleName] = Seq(FlexibleName.Any())
    ) extends ResetApplicationOptions with ApplicationTopicOffsetsOptions {
      def topicPartitionPositionFor(topologyNamespace: Namespace, topicId: TopicId, partition: Int): Option[TopicPartitionPosition] =
        if (topics.map(_.withTopologyNamespace(topologyNamespace)).exists(_.doesMatch(topicId))) Some(position) else None
      def expectExistingTopicPartitions(topologyNamespace: Namespace): Iterable[(TopicId, Option[Int])] = Iterable.empty
    }

    // TODO if we want to support local or fully qualified topic ids, it shouldn't be too hard: we just need a new TopicId type to represent it, and try both in the topicPartitionPositionFor and expectExistingTopicPartitions methods
    final case class ApplicationTopicPositions(
      positions: Map[LocalTopicId, TopicPartitionPosition],
      deleteKafkaStreamsInternalTopics: Iterable[FlexibleName] = Seq(FlexibleName.Any())
    ) extends ResetApplicationOptions with ApplicationTopicOffsetsOptions {
      def fullyQualifiedPositions(topologyNamespace: Namespace): Map[TopicId, TopicPartitionPosition] =
        positions.map { case (ltid, tpp) => (topologyNamespace.appendTopicId(ltid), tpp) }
      def topicPartitionPositionFor(topologyNamespace: Namespace, topicId: TopicId, partition: Int): Option[TopicPartitionPosition] =
        fullyQualifiedPositions(topologyNamespace).get(topicId)
      def expectExistingTopicPartitions(topologyNamespace: Namespace): Iterable[(TopicId, Option[Int])] =
        fullyQualifiedPositions(topologyNamespace).keys.map((_, None))
    }

    // TODO if we want to support local or fully qualified topic ids, it shouldn't be too hard: we just need a new TopicId type to represent it, and try both in the topicPartitionPositionFor and expectExistingTopicPartitions methods
    final case class ApplicationTopicPartitions(
      positions: Map[LocalTopicId, Seq[TopicPartitionPositionOfPartition]],
      deleteKafkaStreamsInternalTopics: Iterable[FlexibleName] = Seq(FlexibleName.Any())
    ) extends ResetApplicationOptions with ApplicationTopicOffsetsOptions {
      def fullyQualifiedPositions(topologyNamespace: Namespace): Map[TopicId, Seq[TopicPartitionPositionOfPartition]] =
        positions.map { case (ltid, tpps) => (topologyNamespace.appendTopicId(ltid), tpps) }
      def topicPartitionPositionFor(topologyNamespace: Namespace, topicId: TopicId, partition: Int): Option[TopicPartitionPosition] =
        fullyQualifiedPositions(topologyNamespace).get(topicId).flatMap(_.find(_.partition == partition).map(_.position))
      def expectExistingTopicPartitions(topologyNamespace: Namespace): Iterable[(TopicId, Option[Int])] =
        fullyQualifiedPositions(topologyNamespace).toSeq.flatMap { case (topicId, tpp) => tpp.map(tppi => (topicId, Some(tppi.partition))) }
    }
  }

  /**
    * Various options for deployed topology application reset.
    */
  final case class ResetOptions(
    application: LocalApplicationId,
    options: ResetApplicationOptions,
    authorizationCode: Option[String] = None
  )
}

object DeployedTopologyStateChange {
  sealed trait StateChange extends SimpleEntityStateChange[DeployedTopology]
  final case class NewVersion(metadata: EntityStateMetadata, entity: DeployedTopology) extends StateChange with SimpleEntityStateChange.NewVersion[DeployedTopology]
  final case class Deleted(metadata: EntityStateMetadata) extends StateChange with SimpleEntityStateChange.Deleted[DeployedTopology]
}
