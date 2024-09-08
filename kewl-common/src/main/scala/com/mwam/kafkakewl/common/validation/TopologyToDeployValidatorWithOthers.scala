/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import com.mwam.kafkakewl.common.validation.Validation._
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology._

import scala.collection.SortedSet

object TopologyToDeployValidatorWithOthers {
  val topologyLikeValidator = new TopologyLikeValidatorWithOthers[TopologyToDeploy.Node, TopologyToDeploy.Topic, TopologyToDeploy.Application, TopologyToDeploy.RelationshipProperties]()
  import topologyLikeValidator._

  def ensureUniqueTopicNames(otherTopologies: Map[TopologyEntityId, TopologyToDeploy], newTopologyId: TopologyEntityId, newTopology: TopologyToDeploy): Validation.Result = {
    val otherTopologiesManagedTopicsByNames = otherTopologies
      .filterKeys(_ != newTopologyId).values
      .flatMap(_.fullyQualifiedTopics.values.map(t => (t.name, t)))
      .toMap

    val newTopologyTopicsThatAlreadyExist = newTopology.fullyQualifiedTopics.values
      .filter(t => otherTopologiesManagedTopicsByNames.contains(t.name))
      .map(_.name)
    if (newTopologyTopicsThatAlreadyExist.nonEmpty)
      newTopologyTopicsThatAlreadyExist.map(t => Validation.Result.validationError(s"Topic '$t' already exists in the kafka cluster")).combine()
    else
      Validation.Result.success
  }

  def ensureUniqueConsumerGroupsAndTransactionalIds(otherTopologies: Map[TopologyEntityId, TopologyToDeploy], newTopologyId: TopologyEntityId, newTopology: TopologyToDeploy): Validation.Result = {
    val applicationConsumerGroupsValidationResult = {
      val otherTopologiesApplicationGroups = otherTopologies.values
        .flatMap(_.fullyQualifiedApplications.values.flatMap(_.actualConsumerGroup))
        .toSet
      val newTopologyApplicationConsumerGroupsThatAlreadyExist = newTopology.fullyQualifiedApplications.values.flatMap(_.actualConsumerGroup).filter(otherTopologiesApplicationGroups.contains)
      if (newTopologyApplicationConsumerGroupsThatAlreadyExist.nonEmpty)
        newTopologyApplicationConsumerGroupsThatAlreadyExist.map(g => Validation.Result.validationError(s"Consumer group '$g' already used in the kafka cluster")).combine()
      else
        Validation.Result.success
    }

    val applicationTransactionalIdsValidationResult = {
      val otherTopologiesApplicationTransactionalIds = otherTopologies.values
        .flatMap(_.fullyQualifiedApplications.values.flatMap(_.actualTransactionalId))
        .toSet
      val newTopologyApplicationConsumerGroupsThatAlreadyExist = newTopology.fullyQualifiedApplications.values.flatMap(_.actualTransactionalId).filter(otherTopologiesApplicationTransactionalIds.contains)
      if (newTopologyApplicationConsumerGroupsThatAlreadyExist.nonEmpty)
        newTopologyApplicationConsumerGroupsThatAlreadyExist.map(t => Validation.Result.validationError(s"Transactional Id '$t' already used in the kafka cluster")).combine()
      else
        Validation.Result.success
    }

    applicationConsumerGroupsValidationResult ++ applicationTransactionalIdsValidationResult
  }

  def validateTopologyWithOthers(
    allowedCustomRelationships: SortedSet[String],
    currentTopologiesMap: Map[TopologyEntityId, TopologyToDeploy],
    newTopologyId: TopologyEntityId,
    newTopologyToDeployOrNone: Option[TopologyToDeploy],
    topicDefaults: TopicDefaults
  ): Validation.Result = {

    // validating the new topology's dependencies (if there is any new topology at all)
    val newTopologyDependenciesValidationResult = newTopologyToDeployOrNone
      .map(newTopologyToDeploy =>
        validateTopologyExternalDependencies(
          allowedCustomRelationships,
          currentTopologiesMap - newTopologyId,
          newTopologyId,
          newTopologyToDeploy,
          nodesIdResolveErrorToString,
          visibilityErrorToString,
          topicDefaults
        ))
      .getOrElse(Validation.Result.success)

    // the new topology may impact the existing topologies' dependencies (e.g. removing a shared topic that's referenced is invalid)
    val existingTopologyDependenciesValidationResult =
      (currentTopologiesMap - newTopologyId)
        .par
        .map { case (existingTopologyId, existingTopology) =>
          validateTopologyExternalDependencies(
            allowedCustomRelationships,
            newTopologyToDeployOrNone
              // pretend that we applied the new topology
              .map(newTopology => currentTopologiesMap - existingTopologyId + (newTopologyId -> newTopology))
              // or if there isn't any, we removed it
              .getOrElse(currentTopologiesMap - newTopologyId - existingTopologyId),
            existingTopologyId,
            existingTopology,
            nodesIdResolveErrorToStringForDependents(existingTopologyId, newTopologyId),
            visibilityErrorToStringForDependents(existingTopologyId, newTopologyId),
            topicDefaults
          )
        }
        .seq
        .toSeq.combine()

    val otherTopologiesMap = currentTopologiesMap - newTopologyId

    Seq(
      newTopologyToDeployOrNone.map(ensureUniqueTopicNames(otherTopologiesMap, newTopologyId, _)).getOrElse(Validation.Result.success),
      newTopologyToDeployOrNone.map(ensureUniqueConsumerGroupsAndTransactionalIds(otherTopologiesMap, newTopologyId, _)).getOrElse(Validation.Result.success),
      newTopologyDependenciesValidationResult,
      existingTopologyDependenciesValidationResult
    ).combine()
  }
}
