/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain.validation

import com.mwam.kafkakewl.domain.*

import scala.util.matching.Regex

object TopologyValidation {
  private val namespaceRegex: Regex = """^[a-zA-Z0-9\-_]+(\.([a-zA-Z0-9\-_]+))*$""".r

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

  private def validateDeployments(deployments: Deployments): ValidationFailures = {
    // Validating all new topologies on their own
    val newTopologiesValidation = deployments.deploy.map(validateTopology).combine

    // Validating the deployment itself
    // TODO implement validating  the deployment, e.g. the deploy/delete topologies can't have duplicates
    val deploymentValidation = valid

    newTopologiesValidation + deploymentValidation
  }

  private def validateTopology(topology: Topology): ValidationFailures = {
    validateTopologyId(topology.id) + validateNamespace(topology.namespace)
  }

  private def validateTopologyDependencies(topology: Topology, topologies: Topologies): ValidationFailures = {

    // TODO implement
    valid
  }

  private def validateNamespace(namespace: Namespace): ValidationFailures = {
    validationErrorIf(s"namespace ${namespace.quote} must not start or end with '.', '_', '_' and can contain only alphanumeric characters and '.', '-', '_'") {
      namespace.value != "" && namespaceRegex.findFirstIn(namespace.value).isEmpty
    }
  }

  private def validateTopologyId(topologyId: TopologyId): ValidationFailures = {
    validationErrorIf(s"topology id ${topologyId.quote} must not be empty") {
      // TODO better topology id validation
      topologyId.value.isEmpty
    }
  }
}
