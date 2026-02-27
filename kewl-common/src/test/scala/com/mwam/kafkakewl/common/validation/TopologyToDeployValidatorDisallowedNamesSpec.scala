/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import com.mwam.kafkakewl.common.ValidationResultMatchers
import com.mwam.kafkakewl.domain.TestTopologiesToDeployCommon
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology._
import org.scalatest.{FlatSpec, Matchers}

class TopologyToDeployValidatorDisallowedNamesSpec extends FlatSpec
  with Matchers
  with ValidationResultMatchers
  with TestTopologiesToDeployCommon
{
  val kafkaCluster: KafkaCluster = KafkaCluster(
    KafkaClusterEntityId("test"),
    "broker1,broker2,broker3"
  )

  val topicDefaults: TopicDefaults = TopicDefaults()

  // --- disallowed developer name regex (resolved TopologyToDeploy) ---

  "topology-to-deploy with no disallowed developer name regex" should "accept any developer names" in {
    val topology = TopologyToDeploy(
      namespace = Namespace("test"),
      developers = Seq("tmp_alice", "bob")
    )
    val result = TopologyToDeployValidator.validateTopology(
      Map.empty, TopologyEntityId("test"), Some(topology),
      kafkaCluster.kafkaCluster, kafkaCluster, topicDefaults, TopologyValidatorConfig.default
    )
    result should beValid
  }

  "topology-to-deploy with disallowed developer name regex" should "reject matching developer names" in {
    val config = TopologyValidatorConfig(disallowedDeveloperNameRegex = Some("^tmp_.+"))
    val topology = TopologyToDeploy(
      namespace = Namespace("test"),
      developers = Seq("tmp_alice", "bob")
    )
    val result = TopologyToDeployValidator.validateTopology(
      Map.empty, TopologyEntityId("test"), Some(topology),
      kafkaCluster.kafkaCluster, kafkaCluster, topicDefaults, config
    )
    result should beInvalid
    result should containMessage("developer names 'tmp_alice' are disallowed (matching regex '^tmp_.+')")
  }

  "topology-to-deploy with disallowed developer name regex" should "reject multiple matching names" in {
    val config = TopologyValidatorConfig(disallowedDeveloperNameRegex = Some("^tmp_.+"))
    val topology = TopologyToDeploy(
      namespace = Namespace("test"),
      developers = Seq("tmp_alice", "tmp_bob", "carol")
    )
    val result = TopologyToDeployValidator.validateTopology(
      Map.empty, TopologyEntityId("test"), Some(topology),
      kafkaCluster.kafkaCluster, kafkaCluster, topicDefaults, config
    )
    result should beInvalid
    result should containMessage("developer names 'tmp_alice', 'tmp_bob' are disallowed (matching regex '^tmp_.+')")
  }

  "topology-to-deploy with disallowed developer name regex" should "accept non-matching developer names" in {
    val config = TopologyValidatorConfig(disallowedDeveloperNameRegex = Some("^tmp_.+"))
    val topology = TopologyToDeploy(
      namespace = Namespace("test"),
      developers = Seq("alice", "bob")
    )
    val result = TopologyToDeployValidator.validateTopology(
      Map.empty, TopologyEntityId("test"), Some(topology),
      kafkaCluster.kafkaCluster, kafkaCluster, topicDefaults, config
    )
    result should beValid
  }

  // --- disallowed application user name regex (resolved TopologyToDeploy) ---

  "topology-to-deploy with no disallowed application user name regex" should "accept any user names" in {
    val topology = TopologyToDeploy(
      namespace = Namespace("test"),
      applications = Map("app1" -> TopologyToDeploy.Application("svc_deprecated")).toMapByApplicationId
    )
    val result = TopologyToDeployValidator.validateTopology(
      Map.empty, TopologyEntityId("test"), Some(topology),
      kafkaCluster.kafkaCluster, kafkaCluster, topicDefaults, TopologyValidatorConfig.default
    )
    result should beValid
  }

  "topology-to-deploy with disallowed application user name regex" should "reject matching user names" in {
    val config = TopologyValidatorConfig(disallowedApplicationUserNameRegex = Some(".*_deprecated$"))
    val topology = TopologyToDeploy(
      namespace = Namespace("test"),
      applications = Map("app1" -> TopologyToDeploy.Application("svc_deprecated")).toMapByApplicationId
    )
    val result = TopologyToDeployValidator.validateTopology(
      Map.empty, TopologyEntityId("test"), Some(topology),
      kafkaCluster.kafkaCluster, kafkaCluster, topicDefaults, config
    )
    result should beInvalid
    result should containMessage("application user names 'svc_deprecated' are disallowed (matching regex '.*_deprecated$')")
  }

  "topology-to-deploy with disallowed application user name regex" should "reject multiple matching user names" in {
    val config = TopologyValidatorConfig(disallowedApplicationUserNameRegex = Some(".*_deprecated$"))
    val topology = TopologyToDeploy(
      namespace = Namespace("test"),
      applications = Map(
        "app1" -> TopologyToDeploy.Application("svc_deprecated"),
        "app2" -> TopologyToDeploy.Application("old_deprecated"),
        "app3" -> TopologyToDeploy.Application("service-active")
      ).toMapByApplicationId
    )
    val result = TopologyToDeployValidator.validateTopology(
      Map.empty, TopologyEntityId("test"), Some(topology),
      kafkaCluster.kafkaCluster, kafkaCluster, topicDefaults, config
    )
    result should beInvalid
    result should containMessage("are disallowed (matching regex '.*_deprecated$')")
  }

  "topology-to-deploy with disallowed application user name regex" should "accept non-matching user names" in {
    val config = TopologyValidatorConfig(disallowedApplicationUserNameRegex = Some(".*_deprecated$"))
    val topology = TopologyToDeploy(
      namespace = Namespace("test"),
      applications = Map("app1" -> TopologyToDeploy.Application("service-active")).toMapByApplicationId
    )
    val result = TopologyToDeployValidator.validateTopology(
      Map.empty, TopologyEntityId("test"), Some(topology),
      kafkaCluster.kafkaCluster, kafkaCluster, topicDefaults, config
    )
    result should beValid
  }

  // --- both regexes configured ---

  "topology-to-deploy with both regexes" should "reject both disallowed developers and application users" in {
    val config = TopologyValidatorConfig(
      disallowedDeveloperNameRegex = Some("^tmp_.+"),
      disallowedApplicationUserNameRegex = Some(".*_deprecated$")
    )
    val topology = TopologyToDeploy(
      namespace = Namespace("test"),
      developers = Seq("tmp_alice"),
      applications = Map("app1" -> TopologyToDeploy.Application("old_deprecated")).toMapByApplicationId
    )
    val result = TopologyToDeployValidator.validateTopology(
      Map.empty, TopologyEntityId("test"), Some(topology),
      kafkaCluster.kafkaCluster, kafkaCluster, topicDefaults, config
    )
    result should beInvalid
    result should containMessages(
      "developer names 'tmp_alice' are disallowed (matching regex '^tmp_.+')",
      "application user names 'old_deprecated' are disallowed (matching regex '.*_deprecated$')"
    )
  }

  "topology-to-deploy with both regexes and all names valid" should "be valid" in {
    val config = TopologyValidatorConfig(
      disallowedDeveloperNameRegex = Some("^tmp_.+"),
      disallowedApplicationUserNameRegex = Some(".*_deprecated$")
    )
    val topology = TopologyToDeploy(
      namespace = Namespace("test"),
      developers = Seq("alice"),
      applications = Map("app1" -> TopologyToDeploy.Application("service-active")).toMapByApplicationId
    )
    val result = TopologyToDeployValidator.validateTopology(
      Map.empty, TopologyEntityId("test"), Some(topology),
      kafkaCluster.kafkaCluster, kafkaCluster, topicDefaults, config
    )
    result should beValid
  }

  // --- deletion (None topology) should not fail ---

  "topology-to-deploy deletion with disallowed regexes" should "be valid" in {
    val config = TopologyValidatorConfig(
      disallowedDeveloperNameRegex = Some("^tmp_.+"),
      disallowedApplicationUserNameRegex = Some(".*_deprecated$")
    )
    val result = TopologyToDeployValidator.validateTopology(
      Map.empty, TopologyEntityId("test"), None,
      kafkaCluster.kafkaCluster, kafkaCluster, topicDefaults, config
    )
    result should beValid
  }

  // --- TopologiesToDeployValidator.validateAllTopologies ---

  "TopologiesToDeployValidator with disallowed developer name regex" should "reject matching names in bulk validation" in {
    val config = TopologyValidatorConfig(disallowedDeveloperNameRegex = Some("^tmp_.+"))
    val topologies = Map(
      TopologyEntityId("test") -> TopologyToDeploy(namespace = Namespace("test"), developers = Seq("tmp_alice")),
      TopologyEntityId("other") -> TopologyToDeploy(namespace = Namespace("other"), developers = Seq("bob"))
    )
    val result = TopologiesToDeployValidator.validateAllTopologies(topologies, kafkaCluster.kafkaCluster, kafkaCluster, topicDefaults, config)
    result should beInvalid
    result should containMessage("developer names 'tmp_alice' are disallowed")
  }

  "TopologiesToDeployValidator with disallowed application user name regex" should "reject matching names in bulk validation" in {
    val config = TopologyValidatorConfig(disallowedApplicationUserNameRegex = Some(".*_deprecated$"))
    val topologies = Map(
      TopologyEntityId("test") -> TopologyToDeploy(
        namespace = Namespace("test"),
        applications = Map("app1" -> TopologyToDeploy.Application("svc_deprecated")).toMapByApplicationId
      ),
      TopologyEntityId("other") -> TopologyToDeploy(
        namespace = Namespace("other"),
        applications = Map("app1" -> TopologyToDeploy.Application("service-active")).toMapByApplicationId
      )
    )
    val result = TopologiesToDeployValidator.validateAllTopologies(topologies, kafkaCluster.kafkaCluster, kafkaCluster, topicDefaults, config)
    result should beInvalid
    result should containMessage("application user names 'svc_deprecated' are disallowed")
  }

  "TopologiesToDeployValidator with no disallowed regex" should "accept all names in bulk validation" in {
    val topologies = Map(
      TopologyEntityId("test") -> TopologyToDeploy(
        namespace = Namespace("test"),
        developers = Seq("tmp_alice"),
        applications = Map("app1" -> TopologyToDeploy.Application("svc_deprecated")).toMapByApplicationId
      )
    )
    val result = TopologiesToDeployValidator.validateAllTopologies(topologies, kafkaCluster.kafkaCluster, kafkaCluster, topicDefaults, TopologyValidatorConfig.default)
    result should beValid
  }
}
