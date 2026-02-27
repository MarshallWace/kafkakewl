/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import com.mwam.kafkakewl.common.topology.TopologyOperations
import com.mwam.kafkakewl.common.validation.Validation._
import com.mwam.kafkakewl.domain.topology._

object TopologyToDeployValidatorStandalone extends ValidationUtils with TopologyOperations {
  val topologyLikeValidator = new TopologyLikeValidatorStandalone[TopologyToDeploy.Node, TopologyToDeploy.Topic, TopologyToDeploy.Application, TopologyToDeploy.RelationshipProperties]()

  def validateStandaloneTopology(topologyId: TopologyEntityId, topology: TopologyToDeploy, validatorConfig: TopologyValidatorConfig): Validation.Result = {
    Seq(
      topologyLikeValidator.validateStandaloneTopology(topologyId, topology, validatorConfig),
      topologyLikeValidator.validateDisallowedApplicationUserNames(topology.applications.values.map(_.user), validatorConfig.disallowedApplicationUserNameRegex)
    ).combine()
  }
}
