/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.topology

import com.mwam.kafkakewl.domain.topology._

import scala.reflect.ClassTag
import cats.syntax.either._
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults

object TopologyLikeOperations {

  /**
    * The possible errors that can happen during resolving nodes-ids (currently only one).
    */
  sealed trait NodeRefResolveError {
    val nodeRef: NodeRef
  }
  object NodeRefResolveError {
    final case class DoesNotExist(nodeRef: NodeRef) extends NodeRefResolveError
    final case class ExpectedFlexibleNodeIdNotFound(nodeRef: NodeRef, notFound: Iterable[String]) extends NodeRefResolveError
  }

  /**
    * The possible visibility errors that can happen when checking relationships' visibility.
    *
    * @tparam NodeType the type of the node base type
    */
  sealed trait RelationshipNodeVisibilityError[NodeType] {
    val node: ResolvedTopologyLikeNode[NodeId, NodeType]

    /**
      * If a relationship doesn't have visibility errors OR has, but they are all filterable -> the invisible relationship itself is filterable.
      *
      * If the error's node it not a specific one (that is, coming from an alias not a specific reference) then we can filter out the non-visible ones simply.
      *
      * @return true if the relationship visibility error can be filtered out (i.e. ignored)
      */
    def canBeFilteredOut: Boolean = !node.specific
  }
  object RelationshipNodeVisibilityError {
    final case class ApplicationCannotConsumeNamespace[NodeType](node: ResolvedTopologyLikeNode[NodeId, NodeType], namespace: Namespace) extends RelationshipNodeVisibilityError[NodeType]
    final case class TopicCannotBeConsumedByNamespace[NodeType](node: ResolvedTopologyLikeNode[NodeId, NodeType], namespace: Namespace) extends RelationshipNodeVisibilityError[NodeType]
    final case class ApplicationCannotProduceNamespace[NodeType](node: ResolvedTopologyLikeNode[NodeId, NodeType], namespace: Namespace) extends RelationshipNodeVisibilityError[NodeType]
    final case class TopicCannotBeProducedByNamespace[NodeType](node: ResolvedTopologyLikeNode[NodeId, NodeType], namespace: Namespace) extends RelationshipNodeVisibilityError[NodeType]
    final case class RelationshipCannotBeExternal[NodeType](node: ResolvedTopologyLikeNode[NodeId, NodeType]) extends RelationshipNodeVisibilityError[NodeType]
  }
}

trait TopologyLikeOperations[NodeType, TopicType <: TopologyLike.Topic with NodeType, ApplicationType <: TopologyLike.Application with NodeType, RelationshipPropertiesType <: TopologyLike.RelationshipProperties] {
  import TopologyLikeOperations._

  type TopologyType = TopologyLike[ApplicationType, TopicType, RelationshipPropertiesType]
  type TopologyNode = TopologyLikeNode[NodeId, NodeType]
  type ResolvedTopologyNode = ResolvedTopologyLikeNode[NodeId, NodeType]

  implicit val topologyIdOrdering: Ordering[TopologyEntityId] = Ordering.by(_.id)
  implicit val topologyNodeIdOrdering: Ordering[NodeId] = Ordering.by(_.id)
  implicit val topologyNodeRefOrdering: Ordering[NodeRef] = Ordering.by(_.id)
  implicit val topologyIdNodeIdOrdering: Ordering[(TopologyEntityId, NodeId)] = Ordering.by { case (tid, nid) => (tid.id, nid.id ) }

  /**
    * Creates a map of all the nodes in the specified topology, keyed by the fully-qualified node id.
    *
    * @param topology the topology whose nodes it'll return
    * @return a map of all the nodes in specified topology
    */
  def allNodesMapOfTopology(topology: TopologyType): Map[NodeId, TopologyNode] = {
    val topologyNamespace = topology.topologyNamespace
    val topologyEntityId = topology.topologyEntityId

    val topics = topology.fullyQualifiedTopics.map {
      case (topicId, topic) => new TopologyNode(topologyEntityId, topologyNamespace, TopologyLikeNodeType.Topic, NodeId(topicId), topic: NodeType)
    }

    val applications = topology.fullyQualifiedApplications.map {
      case (applicationId, application) => new TopologyNode(topologyEntityId, topologyNamespace, TopologyLikeNodeType.Application, NodeId(applicationId), application: NodeType)
    }

    (topics ++ applications)
      .map(n => (n.nodeId, n))
      .toMap
  }

  /**
    * Creates a map of all the nodes in all current topologies, keyed by the fully-qualified node id.
    *
    * @param currentTopologies the topologies
    * @return a map of all the nodes in all current topologies
    */
  def allNodesMapOfTopologies(
    currentTopologies: Iterable[TopologyType]
  ): Map[NodeId, TopologyNode] =
    currentTopologies
      .map(allNodesMapOfTopology)
      .foldLeft(Map.empty[NodeId, TopologyNode])(_ ++ _)

  /**
    * Returns a function that can find all topology nodes for a given nodes-id with.
    *
    * @param allNodesMap the map of all nodes in all topologies
    * @param topology the topology whose aliases have to be taken into account
    * @return a function that can find all topology nodes for a given nodes-id with
    */
  def resolveNodeRefFunc(
    allNodesMap: Map[NodeId, TopologyNode],
    topology: TopologyType
  ): NodeRef => Option[Either[NodeRefResolveError, Iterable[ResolvedTopologyNode]]] = {

    val allNodesMapWithLocal = allNodesMap ++ allNodesMapOfTopology(topology)

    def resolveSingle(nodeRef: NodeRef, nodeId: NodeId): Option[Either[NodeRefResolveError, Iterable[ResolvedTopologyNode]]] =
      // this always succeeds if we found the node, but specific = true => can't be filtered out later due to visibility rules (if it's not visible that's an error)
      allNodesMapWithLocal.get(nodeId).map(n => Iterable(n.toResolved(specific = true)).asRight)

    def resolveAlias(
      aliasMap: Aliases.AliasesMap,
      nodeFilterPredicate: TopologyLikeNode[NodeId, NodeType] => Boolean
    )(nodeRef: NodeRef, aliasId: AliasId): Option[Either[NodeRefResolveError, Iterable[ResolvedTopologyNode]]] =
      aliasMap.get(aliasId).map { tf =>
        val matchingNodes = allNodesMapWithLocal.values
          .filter(nodeFilterPredicate)
          .map(n => (n, tf.doesMatchMatchers(n.nodeId)))
          .collect {
            case (n, matchers) if matchers.nonEmpty =>
              // returning the nodes that we matched with at least one matcher, also returning the matchers
              // note that the specific flag will be true if any of the matching matcher is expected to be matched - in this case we can't filter out the relationship later
              // that was generated from this alias
              (n.toResolved(specific = matchers.exists(_.expectToMatch)), matchers)
          }

        // collecting the matchers in this alias that we managed to match
        val matchingMatchers = matchingNodes
          .map { case (_, matchers) => matchers.toSet }
          .foldLeft(Set.empty[FlexibleNodeId])(_ ++ _)
        val expectedToMatchMatchers = tf.filter(_.expectToMatch).toSet
        // the ones we were expecting to match (Exact ones for instance), but couldn't
        val missingExpectedToMatchMatchers = expectedToMatchMatchers -- matchingMatchers

        Either.cond(
          missingExpectedToMatchMatchers.isEmpty,
          matchingNodes.map { case (n, _) => n }.toSeq.sortBy(_.topologyNode.nodeId),
          // if we didn't match any of the expected ones, that's an error
          NodeRefResolveError.ExpectedFlexibleNodeIdNotFound(nodeRef, missingExpectedToMatchMatchers.map(_.toString))
        )
      }

    val resolveTopicAlias = resolveAlias(topology.fullyQualifiedAliases.topics, _.isTopic) _
    val resolveApplicationAlias = resolveAlias(topology.fullyQualifiedAliases.applications, _.isApplication) _

    nodeRef => {
      {
        // try it as a single local node
        resolveSingle(nodeRef, topology.fullyQualifiedNodeId(LocalNodeId(nodeRef))) orElse
        // then try it as a single fully-qualified node
        resolveSingle(nodeRef, NodeId(nodeRef)) orElse
        // then try it as a local topic or application alias id
        {
          val fullyQualifiedAliasId = topology.fullyQualifiedAliasId(LocalAliasId(nodeRef))
          resolveTopicAlias(nodeRef, fullyQualifiedAliasId) orElse resolveApplicationAlias(nodeRef, fullyQualifiedAliasId)
        } orElse
        // then try it as a full-qualified topic or application alias id
        resolveTopicAlias(nodeRef, AliasId(nodeRef)) orElse resolveApplicationAlias(nodeRef, AliasId(nodeRef))
      }
    }
  }

  /**
    * Collects and returns all the resolved relationships in the given topology using the resolver function.
    *
    * @param resolveNodeRefFunc the resolver to find nodes
    * @param relationships the topology's relationships we need to resolve
    * @return all the resolution errors and all the resolved relationships in the given topology
    */
  def collectAllRelationships(
    resolveNodeRefFunc: NodeRef => Option[Either[NodeRefResolveError, Iterable[ResolvedTopologyNode]]],
    relationships: Iterable[(NodeRef, TopologyLike.NodeRelationships[RelationshipPropertiesType])]
  ): (Seq[NodeRefResolveError], Seq[TopologyLike.TopologyNodeNodeRelationship[NodeType, RelationshipPropertiesType]]) = {
    val nodesIdRelationships = for {
      (nodesId1, nodesId1Relationships) <- relationships
      (relationshipType, nodeRelationships) <- nodesId1Relationships
      nodeRelationship <- nodeRelationships
      nodesId2 = nodeRelationship.id
      relationshipProperties = nodeRelationship.properties
    } yield (nodesId1, relationshipType, relationshipProperties, nodesId2)

    // resolving all nodes-ids, collecting errors
    val allNodeRefs = nodesIdRelationships.flatMap { case (n1, _, _, n2) => Iterable(n1, n2) }.toSeq.distinct.sorted
    val resolvedNodeRefs = allNodeRefs.map(nid => (nid, resolveNodeRefFunc(nid)))
    // separating the errors...
    val nodesIdsResolveErrors = resolvedNodeRefs.collect {
      case (nid, None) => NodeRefResolveError.DoesNotExist(nid)
      case (_, Some(Left(e))) => e
    }
    // ...and the successes
    val successfullyResolvedNodeRefs = resolvedNodeRefs.collect {
      case (nid, Some(Right(r))) => (nid, r)
    }.toMap

    // now we can focus on the ones that we could resolve successfully, ignoring the failures (we collected the failures already)
    val resolvedRelationships = for {
      (nodesId1, relationshipType, relationshipProperties, nodesId2) <- nodesIdRelationships
      resolvedNode1 <- successfullyResolvedNodeRefs.getOrElse(nodesId1, Iterable.empty) // ignoring unresolvable ones
      resolvedNode2 <- successfullyResolvedNodeRefs.getOrElse(nodesId2, Iterable.empty) // ignoring unresolvable ones
    } yield TopologyLike.TopologyNodeNodeRelationship(
      resolvedNode1,
      resolvedNode2,
      relationshipType,
      relationshipProperties
    )

    (nodesIdsResolveErrors, resolvedRelationships.toSeq)
  }

  /**
    * Returns an empty iterable if the relationship is visible in the specified topology, otherwise a set of visibility errors.
    *
    * @param topologyId the topology id
    * @param topology the topology where the relationship is
    * @param relationship the relationship
    * @param topicDefaults the topic defaults
    * @return an empty seq if the relationship is visible in the specified topology, otherwise a seq of visibility errors
    */
  def isRelationshipVisibleFor(topologyId: TopologyEntityId, topology: TopologyType, topicDefaults: TopicDefaults)(
    relationship: TopologyLike.TopologyNodeNodeRelationship[NodeType, RelationshipPropertiesType],
  ): Seq[RelationshipNodeVisibilityError[NodeType]] = {

    def applicationCanConsumeNamespace(isExternal: Boolean, node: ResolvedTopologyNode, application: TopologyLike.Application, namespace: Namespace) =
      if (!isExternal || application.canConsumeNamespace(namespace)) None
      else Some(RelationshipNodeVisibilityError.ApplicationCannotConsumeNamespace(node, namespace))

    def applicationCanProduceNamespace(isExternal: Boolean, node: ResolvedTopologyNode, application: TopologyLike.Application, namespace: Namespace) =
      if (!isExternal || application.canProduceNamespace(namespace)) None
      else Some(RelationshipNodeVisibilityError.ApplicationCannotProduceNamespace(node, namespace))

    def topicCanBeConsumedByNamespace(isExternal: Boolean, node: ResolvedTopologyNode, topic: TopologyLike.Topic, namespace: Namespace, topicDefaults: TopicDefaults) =
      if (!isExternal || topic.canBeConsumedByNamespace(namespace, topicDefaults)) None
      else Some(RelationshipNodeVisibilityError.TopicCannotBeConsumedByNamespace(node, namespace))

    def topicCanBeProducedByNamespace(isExternal: Boolean, node: ResolvedTopologyNode, topic: TopologyLike.Topic, namespace: Namespace, topicDefaults: TopicDefaults) =
      if (!isExternal || topic.canBeProducedByNamespace(namespace, topicDefaults)) None
      else Some(RelationshipNodeVisibilityError.TopicCannotBeProducedByNamespace(node, namespace))

    val topologyNamespace = topology.topologyNamespace

    // the external visibility rules allow only "application consume/produce topic" kind-of relationships with one or both of them being external
    // for any other combination, the external part is considered non-visible, hence the whole relationship

    val node1 = relationship.resolveNode1.topologyNode.node
    val topologyId1 = relationship.resolveNode1.topologyNode.topologyId
    val node1External = topologyId1 != topologyId
    val node1TopologyNamespace = relationship.resolveNode1.topologyNode.topologyNamespace

    val node2 = relationship.resolveNode2.topologyNode.node
    val topologyId2 = relationship.resolveNode2.topologyNode.topologyId
    val node2External = topologyId2 != topologyId
    val node2TopologyNamespace = relationship.resolveNode2.topologyNode.topologyNamespace

    val relationshipType = relationship.relationship

    (node1, node2, relationshipType) match {
      // standard application consumes topic relationship
      case (node1Application: TopologyLike.Application, node2Topic: TopologyLike.Topic, _: RelationshipType.Consume) =>
        Seq(
          // if both ends are external we require both the current topology's namespace and the other end to be able to have the relationship
          applicationCanConsumeNamespace(node1External, relationship.resolveNode1, node1Application, topologyNamespace),
          applicationCanConsumeNamespace(node1External, relationship.resolveNode1, node1Application, node2TopologyNamespace),
          topicCanBeConsumedByNamespace(node2External, relationship.resolveNode2, node2Topic, topologyNamespace, topicDefaults),
          topicCanBeConsumedByNamespace(node2External, relationship.resolveNode2, node2Topic, node1TopologyNamespace, topicDefaults),
        ).flatten.distinct

      // standard application produces topic relationship
      case (node1Application: TopologyLike.Application, node2Topic: TopologyLike.Topic, _: RelationshipType.Produce) =>
        Seq(
          // if both ends are external we require both the current topology's namespace and the other end to be able to have the relationship
          applicationCanProduceNamespace(node1External, relationship.resolveNode1, node1Application, topologyNamespace),
          applicationCanProduceNamespace(node1External, relationship.resolveNode1, node1Application, node2TopologyNamespace),
          topicCanBeProducedByNamespace(node2External, relationship.resolveNode2, node2Topic, topologyNamespace, topicDefaults),
          topicCanBeProducedByNamespace(node2External, relationship.resolveNode2, node2Topic, node1TopologyNamespace, topicDefaults),
        ).flatten.distinct

      // non-standard relationships (app-app, topic-topic, or topic-app)
      case _ =>
        Seq(
          // either external end generates a visibility error
          if (node1External) Some(RelationshipNodeVisibilityError.RelationshipCannotBeExternal(relationship.resolveNode1)) else None,
          if (node2External) Some(RelationshipNodeVisibilityError.RelationshipCannotBeExternal(relationship.resolveNode2)) else None
        ).flatten.distinct
    }
  }

  /**
    * Collects and returns all the resolved and visible relationships in the given topology using the resolver function.
    *
    * @param topologyId the topology id
    * @param topology the topology where the relationship is
    * @param resolveNodeRefFunc the resolver to find nodes
    * @param topicDefaults the topic defaults
    * @return all the resolution, visibility errors and all the resolved relationships in the given topology
    */
  def collectAllVisibleRelationshipsFor(
    topologyId: TopologyEntityId,
    topology: TopologyType,
    resolveNodeRefFunc: NodeRef => Option[Either[NodeRefResolveError, Iterable[ResolvedTopologyNode]]],
    topicDefaults: TopicDefaults
  ): (Seq[NodeRefResolveError], Seq[RelationshipNodeVisibilityError[NodeType]], Seq[TopologyLike.TopologyNodeNodeRelationship[NodeType, RelationshipPropertiesType]]) = {
    // resolving all the relationships
    val (resolveErrors, resolvedRelationships) = collectAllRelationships(resolveNodeRefFunc, topology.relationships)

    // determining relationship visibilities
    val resolvedRelationshipVisibilities = resolvedRelationships.map(r => (r, isRelationshipVisibleFor(topologyId, topology, topicDefaults)(r)))

    // collecting the visibility errors
    val relationshipsVisibilityErrors = resolvedRelationshipVisibilities
      .flatMap { case (_, errors) => errors.filterNot(_.canBeFilteredOut) }
      .distinct

    // collecting the nodes that have no visibility errors
    val visibleResolvedRelationships = resolvedRelationshipVisibilities
      .collect { case (relationship, errors) if errors.isEmpty => relationship }

    (resolveErrors, relationshipsVisibilityErrors, visibleResolvedRelationships)
  }

  /**
    * Collects and returns all the resolved and visible relationships in the given topology using the resolver function.
    *
    * It doesn't return the resolve/visibility errors.
    *
    * @param topologyId the topology id
    * @param topology the topology where the relationship is
    * @param resolveNodeRefFunc the resolver to find nodes
    * @param topicDefaults the topic defaults
    * @return all the resolved relationships in the given topology
    */
  def collectAllVisibleRelationshipsWithoutErrorsFor(
    topologyId: TopologyEntityId,
    topology: TopologyType,
    resolveNodeRefFunc: NodeRef => Option[Either[NodeRefResolveError, Iterable[ResolvedTopologyNode]]],
    topicDefaults: TopicDefaults
  ): Iterable[TopologyLike.TopologyNodeNodeRelationship[NodeType, RelationshipPropertiesType]] = {
    val (_, _, visibleRelationships) = collectAllVisibleRelationshipsFor(topologyId, topology, resolveNodeRefFunc, topicDefaults)
    visibleRelationships
  }


  /**
    * Converts the relationship to an application - topic relationship if possible.
    *
    * @param relationship the relationship
    * @param act the class-tag of the application type
    * @param tct the class-tag of the topic type
    * @return an ApplicationTopicRelationship or None
    */
  def resolveToApplicationTopicRelationship(
    relationship: TopologyLike.TopologyNodeNodeRelationship[NodeType, RelationshipPropertiesType]
  )(
    implicit
    act: ClassTag[ApplicationType],
    tct: ClassTag[TopicType]
  ): Option[TopologyLike.TopologyApplicationTopicRelationship[ApplicationType, TopicType, RelationshipPropertiesType]] = {
    (relationship.resolveNode1.asApplicationOrNone[ApplicationType], relationship.resolveNode2.asTopicOrNone[TopicType]) match {
      case (Some(applicationNode), Some(topicNode)) => Some(
        TopologyLike.TopologyApplicationTopicRelationship(
          applicationNode,
          topicNode,
          relationship.relationship,
          relationship.properties
        )
      )
      case _ => None
    }
  }

  /**
    * Returns the relationships of the specified application (including relationships defined in other topologies).
    *
    * @param currentTopologies the current topologies (to find external nodes)
    * @param topologyId the topology id
    * @param topology the topology we want to resolve in
    * @param applicationId the application id
    * @param act the class-tag of the application type
    * @param tct the class-tag of the topic type
    * @return the relationships of the specified application (including relationships defined in other topologies)
    */
  def allRelationshipsOfApplication(
    currentTopologies: Map[TopologyEntityId, TopologyType],
    topologyId: TopologyEntityId,
    topology: TopologyType,
    applicationId: ApplicationId,
    topicDefaults: TopicDefaults
  )(
    implicit
    act: ClassTag[ApplicationType],
    tct: ClassTag[TopicType]
  ): Iterable[TopologyLike.TopologyApplicationTopicRelationship[ApplicationType, TopicType, RelationshipPropertiesType]] = {
    val allNodesMap = allNodesMapOfTopologies(currentTopologies.values)
    val resolveFunc = resolveNodeRefFunc(allNodesMap, topology)

    val relationships =
      for {
        application <- topology.fullyQualifiedApplications.get(applicationId)
      } yield {
        val localRelationships = collectAllVisibleRelationshipsWithoutErrorsFor(topologyId, topology, resolveFunc, topicDefaults)

        val otherRelationships =
          if (application.canBeReferenced) {
            // TODO here could optimize by collecting the relationships of the topologies that can actually reference this application
            currentTopologies
              .collect {
                case (otherTopologyId, otherTopology) if otherTopologyId != topologyId =>
                  val otherRelationships = collectAllVisibleRelationshipsWithoutErrorsFor(otherTopologyId, otherTopology, resolveFunc, topicDefaults)
                  otherRelationships
              }
              .flatten
          } else {
            Iterable.empty
          }

        val allRelationships = localRelationships ++ otherRelationships

        // filtering for application-topic relationships of this application
        allRelationships
          .flatMap(resolveToApplicationTopicRelationship(_))
          .filter(_.resolveNode1.topologyNode.nodeId == applicationId)
      }
    relationships.getOrElse(Iterable.empty)
  }
}
