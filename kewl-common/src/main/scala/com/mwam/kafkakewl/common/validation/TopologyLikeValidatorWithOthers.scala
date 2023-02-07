/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import com.mwam.kafkakewl.common.topology.TopologyLikeOperations
import com.mwam.kafkakewl.common.validation.Validation._
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology._
import com.mwam.kafkakewl.utils._

import scala.collection.SortedSet
import scala.reflect.ClassTag

class TopologyLikeValidatorWithOthers[NodeType, TopicType <: TopologyLike.Topic with NodeType : ClassTag, ApplicationType <: TopologyLike.Application with NodeType : ClassTag, RelationshipPropertiesType <: TopologyLike.RelationshipProperties]
  extends TopologyLikeOperations[NodeType, TopicType, ApplicationType, RelationshipPropertiesType] {

  def validateTopologyRelationship(
    allowedCustomRelationships: SortedSet[String],
    topologyId: TopologyEntityId,
    topology: TopologyType,
    nodesIdResolveErrorToStringFunc: TopologyLikeOperations.NodeRefResolveError => String
  )(relationship: (NodeRef, TopologyLike.NodeRelationships[RelationshipPropertiesType])): Result = {
    val (nodeRef, nodeRelationship) = relationship

    val monitorConsumerLagValidationResults = nodeRelationship
      .toSeq
      .flatMap { case (r, rps) => rps.map(rp => (r, rp)) }
      .filter { case (r, rp) => !r.isConsume && rp.properties.monitorConsumerLag.isDefined }
      .map { case (r, rp) => Result.validationError(s"${nodeRef.quote}: monitorConsumerLag cannot be set for ${r.toString.quote} ${rp.id.quote} relationship") }

    // validating the custom relationship-types
    val notAllowedCustomRelationshipTypes = nodeRelationship.keySet
      .collect { case r: RelationshipType.Custom if !allowedCustomRelationships(r.toString) => r }
      .map(_.toString)
    val relationshipTypesValidationResult =
      Result.validationErrorIf(
        notAllowedCustomRelationshipTypes.nonEmpty,
        s"not allowed relationship types: ${notAllowedCustomRelationshipTypes.map(_.quote).mkString(", ")} - only ${(RelationshipType.nonCustom ++ allowedCustomRelationships).map(_.quote).mkString(", ")} are allowed"
      )

    (monitorConsumerLagValidationResults ++ Seq(relationshipTypesValidationResult)).combine()
  }

  def validateRelationship(
    topologyId: TopologyEntityId,
    topology: TopologyType
  )(relationship: TopologyLike.TopologyNodeNodeRelationship[NodeType, RelationshipPropertiesType]): Result = {

    def validateApplicationTopicRelationship(
      consumeProduceRelationshipType: ConsumeProduceRelationshipType,
      applicationTopicRelationship: TopologyLike.TopologyApplicationTopicRelationship[ApplicationType, TopicType, RelationshipPropertiesType]
    ): Result = {
      val applicationId = applicationTopicRelationship.resolveNode1.topologyNode.nodeId
      val application = applicationTopicRelationship.resolveNode1.topologyNode.node
      val topicId = applicationTopicRelationship.resolveNode2.topologyNode.nodeId
      Result.validationErrorIf(
        consumeProduceRelationshipType.isConsume && (application.isSimple || application.isConnector) && application.actualConsumerGroup.isEmpty,
        s"application ${applicationId.quote} does not have a consumer group so it can't consume ${topicId.quote}"
      )
    }

    val consumeProduceRelationshipValidationResult = {
      relationship.relationship match {
        case cpr: ConsumeProduceRelationshipType =>
          // consume or produce relationships must start with an application
          if (!relationship.resolveNode1.topologyNode.isApplication) {
            Result.validationError(
              s"consume or produce relationships must start with an application but it was a ${relationship.resolveNode1.topologyNode.nodeType}: ${relationship.resolveNode1.topologyNode.nodeId.quote}"
            )
          } else {
            resolveToApplicationTopicRelationship(relationship)
              .map(validateApplicationTopicRelationship(cpr, _))
              .getOrElse(Result.success)
          }

        case _ =>
          // custom relationships are not validated here - we don't care
          Result.success
      }
    }

    consumeProduceRelationshipValidationResult
  }

  def validateTopologyExternalDependencies(
    allowedCustomRelationships: SortedSet[String],
    otherTopologiesMap: Map[TopologyEntityId, TopologyType],
    topologyId: TopologyEntityId,
    topology: TopologyType,
    nodesIdResolveErrorToStringFunc: TopologyLikeOperations.NodeRefResolveError => String,
    visibilityErrorToStringFunc: TopologyLikeOperations.RelationshipNodeVisibilityError[NodeType] => String,
    topicDefaults: TopicDefaults
  ): Validation.Result = {
    assert(!otherTopologiesMap.contains(topologyId), s"otherTopologies $otherTopologiesMap must not contain the one we're validating '$topologyId'")

    // validating the relationships' without resolving them (relationship types, etc...)
    val relationshipsValidationResult = topology.relationships
      .map(validateTopologyRelationship(allowedCustomRelationships, topologyId, topology, nodesIdResolveErrorToStringFunc))
      .combine()

    // resolving the relationships
    val allNodesMap = allNodesMapOfTopologies(otherTopologiesMap.values)
    val resolveFunc = resolveNodeRefFunc(allNodesMap, topology)
    val (resolveErrors, relationshipsVisibilityErrors, resolvedRelationships) = collectAllVisibleRelationshipsFor(topologyId, topology, resolveFunc, topicDefaults)

    val duplicateResolvedRelationships = resolvedRelationships
      .map(r => (r.resolveNode1.topologyNode.nodeId, r.relationship, r.resolveNode2.topologyNode.nodeId))
      .duplicates
      .map { case (node1, relationship, node2) => s"${node1.quote}-$relationship-${node2.quote}" }

    // finally validating all the resolved node-node relationships
    val nodeNodeRelationshipsValidationResult = resolvedRelationships.map(validateRelationship(topologyId, topology)).combine()

    Seq(
      relationshipsValidationResult,
      (resolveErrors.map(nodesIdResolveErrorToStringFunc) ++ relationshipsVisibilityErrors.map(visibilityErrorToStringFunc))
        .map(Validation.Result.validationError).combine(),
      nodeNodeRelationshipsValidationResult,
      Result.validationErrorIf(
        duplicateResolvedRelationships.nonEmpty,
        s"duplicate relationships after resolving: ${duplicateResolvedRelationships.mkString(", ")}"
      )
    ).combine()
  }

  /**
    * Converts the TopologyLikeOperations.NodeRefResolveError to an error string, when it's about the currently validated topology.
    *
    * @param error the error
    * @return the error string
    */
  def nodesIdResolveErrorToString(error: TopologyLikeOperations.NodeRefResolveError): String = error match {
    case TopologyLikeOperations.NodeRefResolveError.DoesNotExist(nodeRef) =>
      s"could not find ${nodeRef.quote} for relationship"
    case TopologyLikeOperations.NodeRefResolveError.ExpectedFlexibleNodeIdNotFound(nodeRef, notFound) =>
      s"could not find ${notFound.map(_.quote).mkString(", ")} for alias ${nodeRef.quote}"
  }

  /**
    * Converts the TopologyLikeOperations.RelationshipNodeVisibilityError to an error string, when it's about the currently validated topology.
    *
    * @param error the error
    * @return the error string
    */
  def visibilityErrorToString(error: TopologyLikeOperations.RelationshipNodeVisibilityError[NodeType]): String = error match {
    case e: TopologyLikeOperations.RelationshipNodeVisibilityError.ApplicationCannotConsumeNamespace[NodeType] =>
      s"relationship's referenced application ${e.node.topologyNode.nodeId.quote} cannot consume topics in ${e.namespace.quote} namespace"
    case e: TopologyLikeOperations.RelationshipNodeVisibilityError.TopicCannotBeConsumedByNamespace[NodeType] =>
      s"relationship's referenced topic ${e.node.topologyNode.nodeId.quote} cannot be consumed by applications in ${e.namespace.quote} namespace"
    case e: TopologyLikeOperations.RelationshipNodeVisibilityError.ApplicationCannotProduceNamespace[NodeType] =>
      s"relationship's referenced application ${e.node.topologyNode.nodeId.quote} cannot produce topics in ${e.namespace.quote} namespace"
    case e: TopologyLikeOperations.RelationshipNodeVisibilityError.TopicCannotBeProducedByNamespace[NodeType] =>
      s"relationship's referenced topic ${e.node.topologyNode.nodeId.quote} cannot be produced by applications in ${e.namespace.quote} namespace"
    case e: TopologyLikeOperations.RelationshipNodeVisibilityError.RelationshipCannotBeExternal[NodeType] =>
      s"relationship's referenced external ${e.node.topologyNode.nodeType} ${e.node.topologyNode.nodeId.quote} cannot be used in relationship (only application consume/produce topic can use external applications/topics)"
  }

  /**
    * Converts the TopologyLikeOperations.NodeRefResolveError to an error string, when it's about other topologies that depend on the currently validated topology.
    *
    * @param existingTopologyId the topology id of the existing topology that depends on the currently validated
    * @param newTopologyId the topology id that's currently being validated
    * @param error the error
    * @return the error string
    */
  def nodesIdResolveErrorToStringForDependents(
    existingTopologyId: TopologyEntityId,
    newTopologyId: TopologyEntityId
  )(error: TopologyLikeOperations.NodeRefResolveError): String = error match {
    case TopologyLikeOperations.NodeRefResolveError.DoesNotExist(nodeRef) =>
      s"topology '$existingTopologyId' depends on topology '$newTopologyId': ${nodeRef.quote}"
    case TopologyLikeOperations.NodeRefResolveError.ExpectedFlexibleNodeIdNotFound(nodeRef, notFound) =>
      s"topology '$existingTopologyId' depends on topology '$newTopologyId': ${notFound.map(_.quote).mkString(", ")} for alias ${nodeRef.quote}"
  }


  /**
    * Converts the TopologyLikeOperations.RelationshipNodeVisibilityError to an error string, when it's about other topologies that depend on the currently validated topology.
    *
    * @param existingTopologyId the topology id of the existing topology that depends on the currently validated
    * @param newTopologyId the topology id that's currently being validated
    * @param error the error
    * @return the error string
    */
  def visibilityErrorToStringForDependents(
    existingTopologyId: TopologyEntityId,
    newTopologyId: TopologyEntityId
  )(error: TopologyLikeOperations.RelationshipNodeVisibilityError[NodeType]): String =
    s"topology '$existingTopologyId' depends on topology '$newTopologyId': ${visibilityErrorToString(error)}"
}
