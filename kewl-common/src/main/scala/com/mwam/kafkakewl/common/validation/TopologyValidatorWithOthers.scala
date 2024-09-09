/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import com.mwam.kafkakewl.common.validation.Validation._
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology.{Topology, TopologyEntityId}

import scala.collection.SortedSet

object TopologyValidatorWithOthers {
  val topologyLikeValidator = new TopologyLikeValidatorWithOthers[Topology.Node, Topology.Topic, Topology.Application, Topology.RelationshipProperties]()
  import topologyLikeValidator._

  def validateTopologyWithOthers(
    allowedCustomRelationships: SortedSet[String],
    currentTopologiesMap: Map[TopologyEntityId, Topology],
    newTopologyId: TopologyEntityId,
    newTopologyOrNone: Option[Topology],
    topicDefaults: TopicDefaults
  ): Validation.Result = {
    // validating the new topology's dependencies (if there is any new topology at all)
    val newTopologyDependenciesValidationResult = newTopologyOrNone
      .map(newTopology =>
        validateTopologyExternalDependencies(
          allowedCustomRelationships,
          currentTopologiesMap - newTopologyId,
          newTopologyId,
          newTopology,
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
            newTopologyOrNone
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

    newTopologyDependenciesValidationResult ++ existingTopologyDependenciesValidationResult
  }
}

