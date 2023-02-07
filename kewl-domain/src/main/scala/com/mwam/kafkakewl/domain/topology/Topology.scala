/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package topology

import com.mwam.kafkakewl.domain.topology.Topology._

/**
  * The topology entity identifier (must be unique globally).
  *
  * @param id the topology entity identifier
  */
final case class TopologyEntityId(id: String) extends AnyVal with EntityId

/**
  * Topology related case classes and traits.
  *
  * These describe the logical topology which doesn't know yet where/if it
  * gets deployed.
  *
  * These can contain expressions using variables that can be resolved only at deployment,
  * as well as many properties that have nothing to do with deployment.
  *
  */

final case class TopologyCompact(
  namespace: Namespace,
  topology: TopologyId,
  description: Option[String],
  developers: Seq[String],
  developersAccess: DevelopersAccessExpr,
  deployWithAuthorizationCode: DeployWithAuthorizationCodeExpr,
  tags: TagsExpr = Seq.empty,
  labels: LabelsExpr = Map.empty
) extends Entity with LabelledWithExpr

final case class Topology(
  namespace: Namespace = Namespace(""),   // defaults to empty string only to make the tests simpler, in reality we shouldn't use the default here
  topology: TopologyId = TopologyId(""),
  description: Option[String] = None,
  environments: DeploymentEnvironments.Variables = DeploymentEnvironments.Variables.empty,
  developers: Seq[String] = Seq.empty,
  developersAccess: DevelopersAccessExpr = DevelopersAccessExpr(Expressions.fromVariable(DeploymentEnvironments.Variables.Builtin.developersAccess)),
  deployWithAuthorizationCode: DeployWithAuthorizationCodeExpr = DeployWithAuthorizationCodeExpr(Expressions.fromVariable(DeploymentEnvironments.Variables.Builtin.deployWithAuthorizationCode)),
  topics: Map[LocalTopicId, Topic] = Map.empty,
  applications: Map[LocalApplicationId, Application] = Map.empty,
  aliases: LocalAliases = LocalAliases(),
  relationships: Relationships = Map.empty,
  tags: TagsExpr = Seq.empty,
  labels: LabelsExpr = Map.empty
) extends Entity
  with TopologyLike[Application, Topic, RelationshipProperties]
  with LabelledWithExpr

object TopologyStateChange {
  sealed trait StateChange extends SimpleEntityStateChange[Topology]
  final case class NewVersion(metadata: EntityStateMetadata, entity: Topology) extends StateChange with SimpleEntityStateChange.NewVersion[Topology]
  final case class Deleted(metadata: EntityStateMetadata) extends StateChange with SimpleEntityStateChange.Deleted[Topology]
}

object Topology {
  def compact(t: Topology) =
    TopologyCompact(t.namespace, t.topology, t.description, t.developers, t.developersAccess, t.deployWithAuthorizationCode, t.tags, t.labels)

  final case class DevelopersAccessExpr(expr: String)
    extends Expressions.CustomExpression[DevelopersAccess](
      expr,
      name = "developersAccess",
      DevelopersAccess.parse,
      "DevelopersAccess",
      default = Some(DevelopersAccess.TopicReadOnly)
    )
  final case class DeployWithAuthorizationCodeExpr(expr: String) extends Expressions.BooleanExpression(expr, name = "deployWithAuthorizationCode", default = Some(false))
  final case class TagExpr(expr: String) extends Expressions.SingleStringExpression(expr, name = "tag", default = Some(""))
  final case class LabelKeyExpr(expr: String) extends Expressions.SingleStringExpression(expr, name = "label's key", default = Some(""))
  final case class LabelValueExpr(expr: String) extends Expressions.SingleStringExpression(expr, name = "label's value", default = Some(""))

  type TagsExpr = Seq[TagExpr]
  type LabelsExpr = Map[LabelKeyExpr, LabelValueExpr]

  trait LabelledWithExpr {
    val tags: TagsExpr
    val labels: LabelsExpr
  }

  final case class PartitionsExpr(expr: String) extends Expressions.IntExpression(expr, name = "partitions", default = Some(1))
  final case class ReplicationFactorExpr(expr: String) extends Expressions.ShortExpression(expr, name = "replicationFactor")
  final case class TopicConfigExpr(expr: String) extends Expressions.SingleStringExpression(expr, name = "topic config's value", default = Some(""))
  final case class ReplicaPlacementExpr(expr: String) extends Expressions.SingleStringExpression(expr, name = "the replica placement")

  sealed trait Node

  final case class Topic(
    name: String,
    unManaged: Boolean = false,
    partitions: PartitionsExpr = PartitionsExpr("1"),
    replicationFactor: Option[ReplicationFactorExpr] = None,
    config: Map[String, TopicConfigExpr] = Map.empty,
    replicaPlacement: Option[ReplicaPlacementExpr] = None,
    description: Option[String] = None,
    otherConsumerNamespaces: Seq[FlexibleName] = Seq.empty,
    otherProducerNamespaces: Seq[FlexibleName] = Seq.empty,
    tags: TagsExpr = Seq.empty,
    labels: LabelsExpr = Map.empty
  ) extends TopologyLike.Topic
    with LabelledWithExpr
    with Node

  final case class UserExpr(expr: String) extends Expressions.SingleStringExpression(expr, name = "user")
  final case class ConsumerLagWindowSecondsExpr(expr: String) extends Expressions.IntExpression(expr, name = "consumerLagWindowSeconds")
  final case class Application(
    user: UserExpr,
    host: Option[String] = None,
    `type`: Application.Type = Application.Type.Simple(),
    description: Option[String] = None,
    otherConsumableNamespaces: Seq[FlexibleName] = Seq.empty,
    otherProducableNamespaces: Seq[FlexibleName] = Seq.empty,
    consumerLagWindowSeconds: Option[ConsumerLagWindowSecondsExpr] = None,
    tags: TagsExpr = Seq.empty,
    labels: LabelsExpr = Map.empty
  ) extends TopologyLike.Application
    with LabelledWithExpr with Node {

    def actualConsumerGroup: Option[String] = {
      `type` match {
        case Application.Type.Simple(consumerGroup, _) => consumerGroup
        case Application.Type.KafkaStreams(kafkaStreamsAppId) => Some(kafkaStreamsAppId)
        case _ => None
      }
    }

    def actualTransactionalId: Option[String] = {
      `type` match {
        case Application.Type.Simple(_, Some(transactionalId)) => Some(transactionalId)
        case Application.Type.KafkaStreams(kafkaStreamsAppId) => Some(kafkaStreamsAppId)
        case _ => None
      }
    }

    def isSimple: Boolean = typeAsSimple.isDefined
    def isKafkaStreams: Boolean = typeAsKafkaStreams.isDefined
    def isConnector: Boolean = typeAsConnector.isDefined
    def isConnectReplicator: Boolean = typeAsConnectReplicator.isDefined

    def simpleConsumerGroup: Option[String] = typeAsSimple.flatMap(_.consumerGroup)
    def simpleTransactionalId: Option[String] = typeAsSimple.flatMap(_.transactionalId)
    def kafkaStreamsAppId: Option[String] = typeAsKafkaStreams.map(_.kafkaStreamsAppId)
    def connector: Option[String] = typeAsConnector.map(_.connector)
    def connectReplicator: Option[String] = typeAsConnectReplicator.map(_.connectReplicator)

    def typeAsSimple: Option[Application.Type.Simple] = `type` match { case t: Application.Type.Simple => Some(t) case _ => None}
    def typeAsKafkaStreams: Option[Application.Type.KafkaStreams] = `type` match { case t: Application.Type.KafkaStreams => Some(t) case _ => None}
    def typeAsConnector: Option[Application.Type.Connector] = `type` match { case t: Application.Type.Connector => Some(t) case _ => None}
    def typeAsConnectReplicator: Option[Application.Type.ConnectReplicator] = `type` match { case t: Application.Type.ConnectReplicator => Some(t) case _ => None}
  }
  object Application {
    sealed trait Type
    object Type {
      final case class Simple(consumerGroup: Option[String] = None, transactionalId: Option[String] = None) extends Type
      final case class KafkaStreams(kafkaStreamsAppId: String) extends Type
      final case class Connector(connector: String) extends Type
      final case class ConnectReplicator(connectReplicator: String) extends Type
    }
  }

  final case class RelationshipProperties(
    reset: Option[ResetMode] = None,
    monitorConsumerLag: Option[Boolean] = None,
    tags: TagsExpr = Seq.empty,
    labels: LabelsExpr = Map.empty
  ) extends TopologyLike.RelationshipProperties with LabelledWithExpr {
    def withNodeRef(nodeRef: NodeRef) = NodeRelationshipProperties(nodeRef, this)
  }

  final case class NodeRelationshipProperties(id: NodeRef, properties: RelationshipProperties = RelationshipProperties())
    extends TopologyLike.NodeRelationshipProperties[RelationshipProperties]

  type NodeRelationships = Map[RelationshipType, Seq[NodeRelationshipProperties]]
  type Relationships = Map[NodeRef, NodeRelationships]

  type NodeIdNodeIdRelationship = TopologyLike.TopologyNodeIdNodeIdRelationship[RelationshipProperties]
  type NodeNodeRelationship = TopologyLike.TopologyNodeNodeRelationship[Node, RelationshipProperties]
  type ApplicationTopicRelationship = TopologyLike.TopologyApplicationTopicRelationship[Application, Topic, RelationshipProperties]

  final case class ResolvedTopology(
    namespace: Namespace,
    topology: TopologyId,
    description: Option[String] = None,
    developers: Seq[String] = Seq.empty,
    developersAccess: DevelopersAccessExpr = DevelopersAccessExpr(Expressions.fromVariable(DeploymentEnvironments.Variables.Builtin.developersAccess)),
    topics: Map[TopicId, Topic] = Map.empty,
    applications: Map[ApplicationId, Application] = Map.empty,
    relationships: Seq[NodeIdNodeIdRelationship] = Seq.empty,
    tags: TagsExpr = Seq.empty,
    labels: LabelsExpr = Map.empty
  ) extends Entity with TopologyLike.ResolvedTopology[Application, Topic, RelationshipProperties] with LabelledWithExpr

  object ResolvedTopology {
    def apply(topology: Topology, relationships: Seq[NodeIdNodeIdRelationship]) =
      new ResolvedTopology(
        topology.namespace,
        topology.topology,
        topology.description,
        topology.developers,
        topology.developersAccess,
        topology.fullyQualifiedTopics,
        topology.fullyQualifiedApplications,
        relationships,
        topology.tags,
        topology.labels
      )
  }

  type ResolvedTopologies = Map[TopologyEntityId, ResolvedTopology]
}
