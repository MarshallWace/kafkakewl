/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain.validation

import com.mwam.kafkakewl.domain.{
  Deployments,
  Namespace,
  Topology,
  TopologyDeployments,
  TopologyId
}

import scala.util.matching.Regex

object TopologyValidation {
  private val namespaceRegex: Regex =
    """^[a-zA-Z0-9\-_]+(\.([a-zA-Z0-9\-_]+))*$""".r

  def validate(
      topologyDeploymentsBefore: TopologyDeployments
  )(deployments: Deployments): ValidationFailures = {
    deployments.deploy.map(validate).combine
    // TODO validate topologies and removed topologies together with topologyDeploymentsBefore (via relationships)
  }

  def validate(topology: Topology): ValidationFailures = {
    validateTopologyId(topology.id) + validateNamespace(topology.namespace)
  }

  def validateNamespace(namespace: Namespace): ValidationFailures = {
    validationErrorIf(
      s"namespace ${namespace.quote} must not start or end with '.', '_', '_' and can contain only alphanumeric characters and '.', '-', '_'"
    ) {
      namespace.value != "" && namespaceRegex
        .findFirstIn(namespace.value)
        .isEmpty
    }
  }

  def validateTopologyId(topologyId: TopologyId): ValidationFailures = {
    validationErrorIf(s"topology id ${topologyId.quote} must not be empty") {
      // TODO better topology id validation
      topologyId.value.isEmpty
    }
  }
}
