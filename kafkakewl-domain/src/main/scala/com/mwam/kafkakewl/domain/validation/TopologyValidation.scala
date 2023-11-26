/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain.validation

import com.mwam.kafkakewl.domain.*
import com.mwam.kafkakewl.utils.CollectionExtensions.*

import scala.annotation.unused
import scala.util.matching.Regex

object TopologyValidation {
  private val namespaceRegex: Regex = """^[a-zA-Z0-9\-_]+(\.([a-zA-Z0-9\-_]+))*$""".r

  def validateTopology(topology: Topology): ValidationFailures = {
    validateTopologyId(topology.id) + validateNamespace(topology.namespace)
  }

  def validateTopologyDependencies(@unused topology: Topology, @unused topologies: Topologies): ValidationFailures = {
    // TODO implement
    valid
  }

  private def validateNamespace(namespace: Namespace): ValidationFailures = {
    validationErrorIf(s"namespace ${namespace.quote} must not start or end with '.' and can contain only alphanumeric characters and '.', '-', '_'") {
      namespace.value != "" && namespaceRegex.findFirstIn(namespace.value).isEmpty
    }
  }

  private def validateTopologyId(topologyId: TopologyId): ValidationFailures = {
    validationErrorIf(s"topology id must not be empty") {
      // TODO better topology id validation
      topologyId.value.isEmpty
    }
  }
}
