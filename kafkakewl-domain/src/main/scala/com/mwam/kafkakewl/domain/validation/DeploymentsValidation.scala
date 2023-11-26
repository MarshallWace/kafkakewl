/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain.validation

import com.mwam.kafkakewl.domain.*
import com.mwam.kafkakewl.utils.CollectionExtensions.*

object DeploymentsValidation {
  import TopologyValidation.*

  def validate(topologiesBefore: Topologies, deployments: Deployments): ValidationFailures = {
    // First validate the deployments itself on its own
    val deploymentValidation = validateDeployments(deployments)

    // Then pretend we applied the new topologies and the removed ones and...
    val topologiesAfter = topologiesBefore ++ deployments.deploy.toMapById -- deployments.delete
    val allTopologiesDependenciesValidation = topologiesAfter
      // ...validate every topology's dependencies (even pre-existing topologies' dependencies may be broken after applying the new deployment)
      .map((tid, topology) => validateTopologyDependencies(topology, topologiesAfter - tid))
      .combine

    deploymentValidation + allTopologiesDependenciesValidation
  }

  def validateDeployments(deployments: Deployments): ValidationFailures = {
    // Validating all new topologies on their own
    val newTopologiesValidation = deployments.deploy.map(validateTopology).combine

    // Validating the deployment itself
    val duplicateDeployTopologyIds = deployments.deploy.duplicatesBy(_.id)
    val uniqueDeployTopologiesValidation = validationErrorIf(s"cannot deploy duplicate topologies ${duplicateDeployTopologyIds.map(_.quote).mkString(", ")}")(duplicateDeployTopologyIds.nonEmpty)
    val duplicateDeleteTopologyIds = deployments.delete.duplicates
    val uniqueDeleteTopologiesValidation = validationErrorIf(s"cannot delete duplicate topologies ${duplicateDeleteTopologyIds.map(_.quote).mkString(", ")}")(duplicateDeleteTopologyIds.nonEmpty)
    val overlappingDeployDeleteTopologyIds = deployments.deploy.map(_.id).toSet intersect deployments.delete.toSet
    val uniqueDeployDeleteTopologiesValidation = validationErrorIf(s"topologies ${overlappingDeployDeleteTopologyIds.map(_.quote).mkString(", ")} cannot be deployed and deleted at the same time")(overlappingDeployDeleteTopologyIds.nonEmpty)

    newTopologiesValidation + uniqueDeployTopologiesValidation + uniqueDeleteTopologiesValidation + uniqueDeployDeleteTopologiesValidation
  }
}
