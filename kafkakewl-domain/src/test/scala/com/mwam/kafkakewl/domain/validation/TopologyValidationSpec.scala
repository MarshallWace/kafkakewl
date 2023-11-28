/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain.validation

import com.mwam.kafkakewl.domain.{Topology, *}
import zio.test.*

object TopologyValidationSpec extends ZIOSpecDefault {
  import TopologyValidation.*

  def spec = suite("Topology validation")(
    suite("Standalone topology validation")(
      test("A topology with an empty id should be invalid") {
        val topology = Topology(id = TopologyId(""), namespace = Namespace(""))
        assert(validateTopology(topology))(invalidAssertion("topology id must not be empty"))
      },
      test("A topology with a non-empty id but with an empty namespace should be valid") {
        assert(validateTopology(emptyValidTopology))(validAssertion)
      },
      test("A topology with the namespace containing valid characters should be valid") {
        val topologies = Seq(
          emptyTopology(namespace = "project"),
          emptyTopology(namespace = "project.package"),
          emptyTopology(namespace = "project.package.sub-package-1"),
          emptyTopology(namespace = "project.package.sub-package-1__some-suffix123456789")
        )
        check(Gen.fromIterable(topologies)) { topology =>
          assert(validateTopology(topology))(validAssertion)
        }
      },
      test("A topology with the namespace containing invalid characters should be invalid") {
        val topologies = Seq(
          emptyTopology(namespace = "."),
          emptyTopology(namespace = ".project"),
          emptyTopology(namespace = "project."),
          emptyTopology(namespace = "project..package"),
          emptyTopology(namespace = "project "),
          emptyTopology(namespace = "project+"),
          emptyTopology(namespace = "project/"),
          emptyTopology(namespace = "project\\")
        )
        check(Gen.fromIterable(topologies)) { topology =>
          assert(validateTopology(topology))(
            invalidAssertion(
              s"namespace '${topology.namespace}' must not start or end with '.' and can contain only alphanumeric characters and '.', '-', '_'"
            )
          )
        }
      }
    ),
    suite("Topology dependencies validation")(
      // TODO implement tests for TopologyValidation.validateTopologyDependencies()
    )
  )
}
