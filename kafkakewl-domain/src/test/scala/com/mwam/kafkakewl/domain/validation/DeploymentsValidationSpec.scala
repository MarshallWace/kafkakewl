/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain.validation

import com.mwam.kafkakewl.domain.*
import zio.test.*

object DeploymentsValidationSpec extends ZIOSpecDefault {
  import DeploymentsValidation.*

  def spec = suite("Deployments validation")(
    test("An empty Deployments should be valid") {
      val deployments = Deployments()
      assert(validateDeployments(deployments))(validAssertion)
    },
    test("Deployments with a single valid topology to deploy should be valid") {
      val deployments = Deployments(deploy = Seq(emptyValidTopology))
      assert(validateDeployments(deployments))(validAssertion)
    },
    test("Deployments with multiple unique topologies to deploy should be valid") {
      val deployments = Deployments(
        deploy = Seq(emptyTopology("test1"), emptyTopology("test2"), emptyTopology("test3"))
      )
      assert(validateDeployments(deployments))(validAssertion)
    },
    test("Deployments with duplicate topologies to deploy should be invalid") {
      val deployment = Deployments(
        deploy = Seq(emptyTopology("test1"), emptyTopology("test1"), emptyTopology("test3"))
      )
      assert(validateDeployments(deployment))(invalidAssertion("cannot deploy duplicate topologies 'test1'"))
    },
    test("Deployments with multiple unique topologies to delete should be valid") {
      val deployments = Deployments(
        delete = Seq(TopologyId("test4"), TopologyId("test5"))
      )
      assert(validateDeployments(deployments))(validAssertion)
    },
    test("Deployments with duplicate topologies to delete should be invalid") {
      val deployments = Deployments(
        delete = Seq(TopologyId("test4"), TopologyId("test4"))
      )
      assert(validateDeployments(deployments))(invalidAssertion("cannot delete duplicate topologies 'test4'"))
    },
    test("Deployments with multiple unique topologies to deploy and delete should be valid") {
      val deployments = Deployments(
        deploy = Seq(emptyTopology("test1"), emptyTopology("test2"), emptyTopology("test3")),
        delete = Seq(TopologyId("test4"), TopologyId("test5"))
      )
      assert(validateDeployments(deployments))(validAssertion)
    },
    test("Deployments with unique but overlapping topologies to deploy and delete should be invalid") {
      val deployments = Deployments(
        deploy = Seq(emptyTopology("test1"), emptyTopology("test2"), emptyTopology("test3")),
        delete = Seq(TopologyId("test3"), TopologyId("test4"), TopologyId("test5"))
      )
      assert(validateDeployments(deployments))(invalidAssertion("topologies 'test3' cannot be deployed and deleted at the same time"))
    }
  )
}
