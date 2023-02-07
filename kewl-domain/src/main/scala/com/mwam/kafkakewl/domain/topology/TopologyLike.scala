/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package topology

import com.mwam.kafkakewl.domain.topology.TopologyLike._

/**
  * Traits to have some abstraction above Topology case classes/traits and TopologyToDeploy case classes/traits.
  *
  * Topology and TopologyToDeploy is a way to separate the topology information about the logical topology (pre-deployment),
  * and the to-be-deployed one (which is going to be deployed to a specific environment).
  *
  * However, at the moment they are very similar, and significant amount of code uses both. To reduce code duplication,
  * these abstractions can help (see TopologyLikeValidator which validates using only these traits here).
  *
  */

trait TopologyLike[ApplicationType <: Application, TopicType <: Topic, RelationshipPropertiesType] {
  val namespace: Namespace
  val topology: TopologyId
  val topics: Map[LocalTopicId, TopicType]
  val applications: Map[LocalApplicationId, ApplicationType]
  val aliases: LocalAliases
  val relationships: Relationships[RelationshipPropertiesType]

  lazy val topologyNamespace = Namespace(namespace, topology)
  lazy val topologyEntityId: TopologyEntityId = TopologyEntityId(topologyNamespace.ns)

  lazy val fullyQualifiedTopics: Map[TopicId, TopicType] = topics.map { case (tid, t) => (fullyQualifiedTopicId(tid), t) }
  lazy val fullyQualifiedApplications: Map[ApplicationId, ApplicationType] = applications.map { case (aid, a) => (fullyQualifiedApplicationId(aid), a) }
  lazy val fullyQualifiedAliases: Aliases = Aliases(
    // even the flexible node ids will be changed so that the exact one supports local or fully qualified matching
    topics = aliases.topics.map { case (nid, a) => (fullyQualifiedAliasId(nid), a.map(_.withTopologyNamespace(topologyNamespace))) },
    applications = aliases.applications.map { case (nid, a) => (fullyQualifiedAliasId(nid), a.map(_.withTopologyNamespace(topologyNamespace))) }
  )

  def fullyQualifiedNodeId(nodeId: LocalNodeId): NodeId = topologyNamespace.appendNodeId(nodeId)
  def fullyQualifiedTopicId(topicId: LocalTopicId): TopicId = topologyNamespace.appendTopicId(topicId)
  def fullyQualifiedApplicationId(applicationId: LocalApplicationId): ApplicationId = topologyNamespace.appendApplicationId(applicationId)
  def fullyQualifiedAliasId(aliasId: LocalAliasId): AliasId = topologyNamespace.appendAliasId(aliasId)
}

/**
  * The possible types of nodes (application or topic for now).
  */
sealed trait TopologyLikeNodeType
object TopologyLikeNodeType {
  final case object Topic extends TopologyLikeNodeType {
    override def toString = "topic"
  }
  final case object Application extends TopologyLikeNodeType {
    override def toString = "application"
  }
}

/**
  * A node (application or topic for now) in a topology.
  *
  * @param topologyId the id of the topology that contains the node
  * @param topologyNamespace the namespace of the topology (fully-qualified)
  * @param nodeType the type of the node
  * @param nodeId the id (fully qualified) of the node
  * @param node the node instance itself
  * @tparam NodeIdType the type of node id type
  * @tparam NodeType the type of node type
  */
final case class TopologyLikeNode[NodeIdType <: IdLike, NodeType](
  topologyId: TopologyEntityId,
  topologyNamespace: Namespace,
  nodeType: TopologyLikeNodeType,
  nodeId: NodeIdType,
  node: NodeType
) {
  def isTopic: Boolean = nodeType == TopologyLikeNodeType.Topic
  def isApplication: Boolean = nodeType == TopologyLikeNodeType.Application
  def toResolved(specific: Boolean): ResolvedTopologyLikeNode[NodeIdType, NodeType] = ResolvedTopologyLikeNode(this, specific)

  def asApplication[ApplicationType <: Application]: TopologyLikeNode[ApplicationId, ApplicationType] =
    copy(nodeId = ApplicationId(nodeId.id), node = node.asInstanceOf[ApplicationType])
  def asTopic[TopicType <: Topic]: TopologyLikeNode[TopicId, TopicType] =
    copy(nodeId = TopicId(nodeId.id), node = node.asInstanceOf[TopicType])
}

/**
  * The resolved node in a topology (the result of resolving a specific NodeRef or an alias).
  *
  * It's effectively a TopologyLikeNode plus some additional info about how it was resolved (specific).
  *
  * @param topologyNode the topology node
  * @param specific true, if the node was referenced specifically that is with an explicit single reference, not an alias
  * @tparam NodeType the type of node base type
  */
final case class ResolvedTopologyLikeNode[NodeIdType <: IdLike, NodeType](topologyNode: TopologyLikeNode[NodeIdType, NodeType], specific: Boolean) {
  def asApplicationOrNone[ApplicationType <: Application]: Option[ResolvedTopologyLikeNode[ApplicationId, ApplicationType]] = topologyNode.nodeType match {
    case TopologyLikeNodeType.Application => Some(copy(topologyNode = topologyNode.asApplication))
    case _ => None
  }

  def asTopicOrNone[TopicType <: Topic]: Option[ResolvedTopologyLikeNode[TopicId, TopicType]] = topologyNode.nodeType match {
    case TopologyLikeNodeType.Topic => Some(copy(topologyNode = topologyNode.asTopic))
    case _ => None
  }
}

object TopologyLike {
  final case class TopicDefaults(
    otherConsumerNamespaces: Seq[FlexibleName] = Seq.empty,
    otherProducerNamespaces: Seq[FlexibleName] = Seq.empty
  ) {
    def withConsumerNamespace(namespace: String*): TopicDefaults =
      copy(
        otherConsumerNamespaces = otherConsumerNamespaces ++ namespace.map(ns => FlexibleName.Namespace(ns))
      )

    def withProducerNamespace(namespace: String*): TopicDefaults =
      copy(
        otherProducerNamespaces = otherProducerNamespaces ++ namespace.map(ns => FlexibleName.Namespace(ns))
      )

    def canBeConsumedByNamespace(namespace: Namespace, topic: Topic): Boolean =
      (topic.otherConsumerNamespaces ++ otherConsumerNamespaces).exists(_.doesMatch(namespace.ns))

    def canBeProducedByNamespace(namespace: Namespace, topic: Topic): Boolean =
      (topic.otherProducerNamespaces ++ otherProducerNamespaces).exists(_.doesMatch(namespace.ns))

    def canBeReferenced(topic: Topic): Boolean = otherConsumerNamespaces.nonEmpty || otherProducerNamespaces.nonEmpty || topic.otherConsumerNamespaces.nonEmpty || topic.otherProducerNamespaces.nonEmpty
  }

  trait Topic {
    val name: String
    val unManaged: Boolean
    val otherConsumerNamespaces: Seq[FlexibleName]
    val otherProducerNamespaces: Seq[FlexibleName]

    def canBeConsumedByNamespace(namespace: Namespace, topicDefaults: TopicDefaults): Boolean = topicDefaults.canBeConsumedByNamespace(namespace, this)
    def canBeProducedByNamespace(namespace: Namespace, topicDefaults: TopicDefaults): Boolean = topicDefaults.canBeProducedByNamespace(namespace, this)
    def canBeReferenced(topicDefaults: TopicDefaults): Boolean = topicDefaults.canBeReferenced(this)
  }

  trait Application {
    val otherConsumableNamespaces: Seq[FlexibleName]
    val otherProducableNamespaces: Seq[FlexibleName]

    def isSimple: Boolean
    def isKafkaStreams: Boolean
    def isConnector: Boolean
    def isConnectReplicator: Boolean

    def actualConsumerGroup: Option[String]

    def simpleConsumerGroup: Option[String]
    def simpleTransactionalId: Option[String]
    def kafkaStreamsAppId: Option[String]
    def connector: Option[String]
    def connectReplicator: Option[String]

    def canConsumeNamespace(namespace: Namespace): Boolean = otherConsumableNamespaces.exists(_.doesMatch(namespace.ns))
    def canProduceNamespace(namespace: Namespace): Boolean = otherProducableNamespaces.exists(_.doesMatch(namespace.ns))
    def canBeReferenced: Boolean = otherConsumableNamespaces.nonEmpty || otherProducableNamespaces.nonEmpty
  }

  trait Relationship {
    val application: ApplicationId
    val topic: TopicId
    def isConsume: Boolean
    def isProduce: Boolean
  }

  trait NodeRelationshipProperties[RelationshipPropertiesType] {
    val id: NodeRef
    val properties: RelationshipPropertiesType
  }

  type NodeRelationships[RelationshipPropertiesType] = Map[RelationshipType, Seq[NodeRelationshipProperties[RelationshipPropertiesType]]]
  type Relationships[RelationshipPropertiesType] = Map[NodeRef, NodeRelationships[RelationshipPropertiesType]]

  trait RelationshipProperties {
    val reset: Option[ResetMode]
    val monitorConsumerLag: Option[Boolean]
  }

  final case class TopologyNodeNodeRelationship[NodeType, RelationshipPropertiesType <: RelationshipProperties](
    resolveNode1: ResolvedTopologyLikeNode[NodeId, NodeType],
    resolveNode2: ResolvedTopologyLikeNode[NodeId, NodeType],
    relationship: RelationshipType,
    properties: RelationshipPropertiesType
  ) {
    def toNodeIdNodeId = TopologyNodeIdNodeIdRelationship(
      resolveNode1.topologyNode.topologyId,
      resolveNode1.topologyNode.nodeId,
      resolveNode2.topologyNode.topologyId,
      resolveNode2.topologyNode.nodeId,
      relationship,
      properties
    )
  }

  final case class TopologyApplicationTopicRelationship[ApplicationType <: Application, TopicType <: Topic, RelationshipPropertiesType <: RelationshipProperties](
    resolveNode1: ResolvedTopologyLikeNode[ApplicationId, ApplicationType],
    resolveNode2: ResolvedTopologyLikeNode[TopicId, TopicType],
    relationship: RelationshipType,
    properties: RelationshipPropertiesType
  ) {
    def monitorConsumerLag: Boolean = relationship.isConsume && properties.monitorConsumerLag.getOrElse(true)
  }

  final case class TopologyNodeIdNodeIdRelationship[RelationshipPropertiesType <: RelationshipProperties](
    topologyId1: TopologyEntityId,
    id1: NodeId,
    topologyId2: TopologyEntityId,
    id2: NodeId,
    relationship: RelationshipType,
    properties: RelationshipPropertiesType
  )

  trait ResolvedTopology[ApplicationType <: Application, TopicType <: Topic, RelationshipPropertiesType <: RelationshipProperties] {
    val namespace: Namespace
    val topology: TopologyId
    val description: Option[String]
    val topics: Map[TopicId, TopicType]
    val applications: Map[ApplicationId, ApplicationType]
    val relationships: Seq[TopologyNodeIdNodeIdRelationship[RelationshipPropertiesType]]

    lazy val topologyNamespace = Namespace(namespace, topology)
    lazy val topologyEntityId: TopologyEntityId = TopologyEntityId(topologyNamespace.ns)
  }

  trait ResolvedTopologies[ApplicationType <: Application, TopicType <: Topic, RelationshipPropertiesType <: RelationshipProperties] {
    val topologies: Map[TopologyEntityId, ResolvedTopology[ApplicationType, TopicType, RelationshipPropertiesType]]
  }
}
