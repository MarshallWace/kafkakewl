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

final case class TopologyValidatorConfig(
  disallowedDeveloperNameRegex: Option[String] = None,
  disallowedReadOnlyDeveloperNameRegex: Option[String] = None,
  disallowedApplicationUserNameRegex: Option[String] = None
)

object TopologyValidatorConfig {
  val default: TopologyValidatorConfig = TopologyValidatorConfig()
}

object TopologyValidator {
  // TODO make it configurable
  val allowedCustomRelationships = SortedSet.empty[String]

  def validateTopology(
    currentTopologiesMap: Map[TopologyEntityId, Topology],
    newTopologyId: TopologyEntityId,
    newTopologyOrNone: Option[Topology],
    topicDefaults: TopicDefaults,
    validatorConfig: TopologyValidatorConfig
  ): Validation.Result = {
    Seq(
      newTopologyOrNone.map(TopologyValidatorStandalone.validateStandaloneTopology(newTopologyId, _, validatorConfig)),
      Some(TopologyValidatorWithOthers.validateTopologyWithOthers(allowedCustomRelationships, currentTopologiesMap, newTopologyId, newTopologyOrNone, topicDefaults))
    ).flatten.combine()
  }
}
