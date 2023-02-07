/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package topology

import com.mwam.kafkakewl.domain.topology.TopologyToDeploy._

/**
  * This is almost a parallel set of case classes and traits for Topology.
  *
  * The purpose of this duplication is to support significant differences between
  * the Topology (the logical) and the TopologyToDeploy (the actual topology that gets
  * deployed to a particular kafka cluster)
  *
  * For example:
  * - Topology supports expression with variables which are resolved in TopologyToDeploy
  * - Topology can support other properties that are not needed for deployment at all
  *
  */

final case class TopologyToDeployCompact(
  namespace: Namespace,
  topology: TopologyId,
  description: Option[String],
  developers: Seq[String],
  developersAccess: DevelopersAccess,
  deployWithAuthorizationCode: Option[Boolean],
  tags: Seq[String] = Seq.empty,
  labels: Map[String, String] = Map.empty
) extends Labelled

final case class TopologyToDeploy(
  namespace: Namespace = Namespace(""),   // defaults to empty string only to make the tests simpler, in reality we shouldn't use the default here
  topology: TopologyId = TopologyId(""),
  description: Option[String] = None,
  developers: Seq[String] = Seq.empty,
  developersAccess: DevelopersAccess = DevelopersAccess.TopicReadOnly,
  deployWithAuthorizationCode: Option[Boolean] = None,  // it's an option to make it backwards compatible, be able to read existing deployed-topologies from json,
                                                        // and fall back to the kafka-clusters requireAuthorizationCode property if it's not there.
                                                        // sooner or later all persisted deployed-topologies will have a value here coming from the topology (and there, possible from the kafka-cluster)
  topics: Map[LocalTopicId, Topic] = Map.empty,
  applications: Map[LocalApplicationId, Application] = Map.empty,
  aliases: LocalAliases = LocalAliases(),
  relationships: Relationships = Map.empty,
  tags: Seq[String] = Seq.empty,
  labels: Map[String, String] = Map.empty
) extends TopologyLike[Application, Topic, RelationshipProperties]
  with Labelled

object TopologyToDeploy {
  def compact(t: TopologyToDeploy) =
    TopologyToDeployCompact(t.namespace, t.topology, t.description, t.developers, t.developersAccess, t.deployWithAuthorizationCode, t.tags, t.labels)

  implicit class TopologyToDeployExtensions(topologyToDeploy: TopologyToDeploy) {
    def topicOrCommandErrors(topicId: TopicId): ValueOrCommandErrors[TopologyToDeploy.Topic] =
      topologyToDeploy.fullyQualifiedTopics
        .get(topicId)
        .toRight(Seq(CommandError.validationError(s"topic ${topicId.quote} does not exist in topology")))

    def applicationOrCommandErrors(applicationId: ApplicationId): ValueOrCommandErrors[TopologyToDeploy.Application] =
      topologyToDeploy.fullyQualifiedApplications
        .get(applicationId)
        .toRight(Seq(CommandError.validationError(s"application ${applicationId.quote} does not exist in topology")))
  }

  sealed trait Node

  final case class Topic(
    name: String,
    unManaged: Boolean = false,
    partitions: Int = 1,
    replicationFactor: Option[Short] = None,
    config: Map[String, String] = Map.empty,
    replicaPlacement: Option[ReplicaPlacementId] = None,
    description: Option[String] = None,
    otherConsumerNamespaces: Seq[FlexibleName] = Seq.empty,
    otherProducerNamespaces: Seq[FlexibleName] = Seq.empty,
    tags: Seq[String] = Seq.empty,
    labels: Map[String, String] = Map.empty
  ) extends TopologyLike.Topic
    with Labelled
    with Node

  final case class Application(
    user: String,
    host: Option[String] = None,
    `type`: Application.Type = Application.Type.Simple(),
    description: Option[String] = None,
    otherConsumableNamespaces: Seq[FlexibleName] = Seq.empty,
    otherProducableNamespaces: Seq[FlexibleName] = Seq.empty,
    consumerLagWindowSeconds: Option[Int] = None,
    tags: Seq[String] = Seq.empty,
    labels: Map[String, String] = Map.empty
  ) extends TopologyLike.Application
    with Labelled
    with Node {

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
    tags: Seq[String] = Seq.empty,
    labels: Map[String, String] = Map.empty
  ) extends TopologyLike.RelationshipProperties with Labelled {
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
    developersAccess: DevelopersAccess = DevelopersAccess.TopicReadOnly,
    topics: Map[TopicId, Topic] = Map.empty,
    applications: Map[ApplicationId, Application] = Map.empty,
    relationships: Seq[NodeIdNodeIdRelationship] = Seq.empty,
    tags: Seq[String] = Seq.empty,
    labels: Map[String, String] = Map.empty
  ) extends Entity with TopologyLike.ResolvedTopology[Application, Topic, RelationshipProperties] with Labelled

  object ResolvedTopology {
    def apply(topology: TopologyToDeploy, relationships: Seq[NodeIdNodeIdRelationship]) =
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
