/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import com.mwam.kafkakewl.common.validation.TopologyValidator.allowedCustomRelationships
import com.mwam.kafkakewl.common.validation.Validation._
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology.{Topology, TopologyEntityId}

object TopologiesValidator {
  val topologyLikeValidator = new TopologyLikeValidatorWithOthers[Topology.Node, Topology.Topic, Topology.Application, Topology.RelationshipProperties]()
  import topologyLikeValidator._

  def validateAllTopologies(
    currentTopologiesMap: Map[TopologyEntityId, Topology],
    topicDefaults: TopicDefaults
  ): Validation.Result = {
    currentTopologiesMap
      .par
      .flatMap { case (topologyId, topology) =>
        Seq(
          // validates the topology on itw own...
          TopologyValidatorStandalone.validateStandaloneTopology(topologyId, topology),
          // ...then validates it against all the others
          validateTopologyExternalDependencies(
            allowedCustomRelationships,
            currentTopologiesMap - topologyId,
            topologyId,
            topology,
            nodesIdResolveErrorToString,
            visibilityErrorToString,
            topicDefaults
          )
          // no need to validate the others with this one, because we'll do that soon enough as part of the main loop
        )
      }
      .seq
      .combine()
  }
}
