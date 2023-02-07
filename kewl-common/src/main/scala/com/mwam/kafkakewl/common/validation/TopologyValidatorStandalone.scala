/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import com.mwam.kafkakewl.common.topology.TopologyOperations
import com.mwam.kafkakewl.domain.topology._

object TopologyValidatorStandalone extends ValidationUtils with TopologyOperations {
  val topologyLikeValidator = new TopologyLikeValidatorStandalone[Topology.Node, Topology.Topic, Topology.Application, Topology.RelationshipProperties]()

  val validateStandaloneTopology = topologyLikeValidator.validateStandaloneTopology _
}
