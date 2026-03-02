/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import com.mwam.kafkakewl.common.ValidationResultMatchers
import com.mwam.kafkakewl.domain.TestTopologiesCommon
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology._
import org.scalatest.{FlatSpec, Matchers}

class TopologyValidatorDisallowedNamesSpec extends FlatSpec
  with Matchers
  with ValidationResultMatchers
  with TestTopologiesCommon
{
  val topicDefaults: TopicDefaults = TopicDefaults()

  def validateTopology(
    newTopologyId: TopologyEntityId,
    newTopology: Topology,
    validatorConfig: TopologyValidatorConfig
  ): Validation.Result = TopologyValidator.validateTopology(Map.empty, newTopologyId, Some(newTopology), topicDefaults, validatorConfig)

  // --- disallowed developer name regex ---

  "topology with no disallowed developer name regex configured" should "accept any developer names" in {
    val topology = Topology(
      Namespace("test"),
      developers = Seq("tmp_alice", "bob", "carol_deprecated")
    )
    val result = validateTopology(TopologyEntityId("test"), topology, TopologyValidatorConfig.default)
    result should beValid
  }

  "topology with disallowed developer name regex" should "reject matching developer names" in {
    val config = TopologyValidatorConfig(disallowedDeveloperNameRegex = Some("^tmp_.+"))
    val topology = Topology(
      Namespace("test"),
      developers = Seq("tmp_alice", "bob")
    )
    val result = validateTopology(TopologyEntityId("test"), topology, config)
    result should beInvalid
    result should containMessage("developer names 'tmp_alice' are disallowed (matching regex '^tmp_.+')")
  }

  "topology with disallowed developer name regex" should "reject multiple matching developer names" in {
    val config = TopologyValidatorConfig(disallowedDeveloperNameRegex = Some("^tmp_.+"))
    val topology = Topology(
      Namespace("test"),
      developers = Seq("tmp_alice", "tmp_bob", "carol")
    )
    val result = validateTopology(TopologyEntityId("test"), topology, config)
    result should beInvalid
    result should containMessage("developer names 'tmp_alice', 'tmp_bob' are disallowed (matching regex '^tmp_.+')")
  }

  "topology with disallowed developer name regex" should "accept non-matching developer names" in {
    val config = TopologyValidatorConfig(disallowedDeveloperNameRegex = Some("^tmp_.+"))
    val topology = Topology(
      Namespace("test"),
      developers = Seq("alice", "bob")
    )
    val result = validateTopology(TopologyEntityId("test"), topology, config)
    result should beValid
  }

  "topology with disallowed developer name regex and no developers" should "be valid" in {
    val config = TopologyValidatorConfig(disallowedDeveloperNameRegex = Some("^tmp_.+"))
    val topology = Topology(Namespace("test"))
    val result = validateTopology(TopologyEntityId("test"), topology, config)
    result should beValid
  }

  // --- application user name regex is NOT validated at Topology level (only at TopologyToDeploy level) ---
  // Application user names in Topology are expressions that may contain variables,
  // so validating them against a disallowed regex would produce false positives.

  "topology with disallowed application user name regex" should "not reject application user names (validated only at deploy time)" in {
    val config = TopologyValidatorConfig(disallowedApplicationUserNameRegex = Some(".*_deprecated$"))
    val topology = Topology(
      Namespace("test"),
      applications = Map("app1" -> Topology.Application("svc_deprecated")).toMapByApplicationId
    )
    val result = validateTopology(TopologyEntityId("test"), topology, config)
    result should beValid
  }

  // --- regex edge cases ---

  "disallowed developer name regex with anchored pattern" should "only match full names" in {
    val config = TopologyValidatorConfig(disallowedDeveloperNameRegex = Some("^tmp_bob$"))
    val topology = Topology(
      Namespace("test"),
      developers = Seq("tmp_bob", "tmp_bob_extended", "pre_tmp_bob")
    )
    val result = validateTopology(TopologyEntityId("test"), topology, config)
    result should beInvalid
    result should containMessage("developer names 'tmp_bob' are disallowed")
  }

  "disallowed developer name regex with partial match" should "match substrings" in {
    val config = TopologyValidatorConfig(disallowedDeveloperNameRegex = Some("tmp"))
    val topology = Topology(
      Namespace("test"),
      developers = Seq("tmp_bob", "use-tmp", "contains-tmp-inside")
    )
    val result = validateTopology(TopologyEntityId("test"), topology, config)
    result should beInvalid
    result should containMessage("developer names 'tmp_bob', 'use-tmp', 'contains-tmp-inside' are disallowed")
  }

  // --- developers and readOnlyDevelopers overlap ---

  "topology with overlapping developers and readOnlyDevelopers" should "be rejected" in {
    val topology = Topology(
      Namespace("test"),
      developers = Seq("alice", "bob"),
      readOnlyDevelopers = Seq("bob", "carol")
    )
    val result = validateTopology(TopologyEntityId("test"), topology, TopologyValidatorConfig.default)
    result should beInvalid
    result should containMessage("developer names 'bob' appear in both developers and readOnlyDevelopers")
  }

  "topology with non-overlapping developers and readOnlyDevelopers" should "be valid" in {
    val topology = Topology(
      Namespace("test"),
      developers = Seq("alice"),
      readOnlyDevelopers = Seq("bob")
    )
    val result = validateTopology(TopologyEntityId("test"), topology, TopologyValidatorConfig.default)
    result should beValid
  }

  // --- duplicate read-only developer names ---

  "topology with duplicate read-only developer names" should "be rejected" in {
    val topology = Topology(
      Namespace("test"),
      readOnlyDevelopers = Seq("bob", "carol", "bob")
    )
    val result = validateTopology(TopologyEntityId("test"), topology, TopologyValidatorConfig.default)
    result should beInvalid
    result should containMessage("duplicate read-only developer names: 'bob'")
  }

  // --- disallowed read-only developer name regex ---

  "topology with no disallowed read-only developer name regex configured" should "accept any read-only developer names" in {
    val topology = Topology(
      Namespace("test"),
      readOnlyDevelopers = Seq("tmp_alice", "bob")
    )
    val result = validateTopology(TopologyEntityId("test"), topology, TopologyValidatorConfig.default)
    result should beValid
  }

  "topology with disallowed read-only developer name regex" should "reject matching read-only developer names" in {
    val config = TopologyValidatorConfig(disallowedReadOnlyDeveloperNameRegex = Some("^tmp_.+"))
    val topology = Topology(
      Namespace("test"),
      readOnlyDevelopers = Seq("tmp_alice", "bob")
    )
    val result = validateTopology(TopologyEntityId("test"), topology, config)
    result should beInvalid
    result should containMessage("read-only developer names 'tmp_alice' are disallowed (matching regex '^tmp_.+')")
  }

  "topology with disallowed read-only developer name regex" should "accept non-matching read-only developer names" in {
    val config = TopologyValidatorConfig(disallowedReadOnlyDeveloperNameRegex = Some("^tmp_.+"))
    val topology = Topology(
      Namespace("test"),
      readOnlyDevelopers = Seq("alice", "bob")
    )
    val result = validateTopology(TopologyEntityId("test"), topology, config)
    result should beValid
  }

  "topology with disallowed read-only developer name regex" should "use its own regex, not the developer one" in {
    val config = TopologyValidatorConfig(
      disallowedDeveloperNameRegex = Some("^tmp_.+"),
      disallowedReadOnlyDeveloperNameRegex = Some("^ro_.+")
    )
    val topology = Topology(
      Namespace("test"),
      developers = Seq("alice"),
      readOnlyDevelopers = Seq("tmp_alice", "ro_bob")
    )
    val result = validateTopology(TopologyEntityId("test"), topology, config)
    result should beInvalid
    result should containMessage("read-only developer names 'ro_bob' are disallowed (matching regex '^ro_.+')")
  }

  // --- TopologyValidatorConfig.default ---

  "TopologyValidatorConfig.default" should "have no restrictions" in {
    val config = TopologyValidatorConfig.default
    config.disallowedDeveloperNameRegex shouldBe None
    config.disallowedReadOnlyDeveloperNameRegex shouldBe None
    config.disallowedApplicationUserNameRegex shouldBe None
  }

  // --- TopologiesValidator.validateAllTopologies ---

  "TopologiesValidator with disallowed developer name regex" should "reject matching names in bulk validation" in {
    val config = TopologyValidatorConfig(disallowedDeveloperNameRegex = Some("^tmp_.+"))
    val topologies = Map(
      TopologyEntityId("test") -> Topology(Namespace("test"), developers = Seq("tmp_alice")),
      TopologyEntityId("other") -> Topology(Namespace("other"), developers = Seq("bob"))
    )
    val result = TopologiesValidator.validateAllTopologies(topologies, topicDefaults, config)
    result should beInvalid
    result should containMessage("developer names 'tmp_alice' are disallowed")
  }

  "TopologiesValidator with disallowed application user name regex" should "not reject application user names (validated only at deploy time)" in {
    val config = TopologyValidatorConfig(disallowedApplicationUserNameRegex = Some(".*_deprecated$"))
    val topologies = Map(
      TopologyEntityId("test") -> Topology(
        Namespace("test"),
        applications = Map("app1" -> Topology.Application("svc_deprecated")).toMapByApplicationId
      )
    )
    val result = TopologiesValidator.validateAllTopologies(topologies, topicDefaults, config)
    result should beValid
  }

  "TopologiesValidator with no disallowed regex" should "accept all names in bulk validation" in {
    val topologies = Map(
      TopologyEntityId("test") -> Topology(
        Namespace("test"),
        developers = Seq("tmp_alice"),
        applications = Map("app1" -> Topology.Application("svc_deprecated")).toMapByApplicationId
      )
    )
    val result = TopologiesValidator.validateAllTopologies(topologies, topicDefaults, TopologyValidatorConfig.default)
    result should beValid
  }
}
