/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import cats.syntax.option._
import com.mwam.kafkakewl.common.ValidationResultMatchers
import com.mwam.kafkakewl.domain.kafka.config.{TopicConfigDefault, TopicConfigDefaults}
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId, emptyReplicaPlacements}
import com.mwam.kafkakewl.domain.topology.ReplicaPlacementId
import com.mwam.kafkakewl.domain.{DeploymentEnvironment, DeploymentEnvironmentId, DeploymentEnvironments}
import org.scalatest.{Matchers, WordSpec}

class KafkaClusterValidatorSpec extends WordSpec
  with Matchers
  with ValidationResultMatchers {

  "KafkaCluster with no environments" should {
    "be invalid" in {
      val kafkaCluster = KafkaCluster(KafkaClusterEntityId("test"), "broker1,broker2,broker3", environments = DeploymentEnvironments.OrderedVariables.empty)
      val actualValidationResult = KafkaClusterValidator.validateKafkaCluster(KafkaClusterEntityId("test"), kafkaCluster)

      actualValidationResult should beInvalid
      actualValidationResult should containMessage("expecting at least one ('default') deployment environment")
    }
  }

  "KafkaCluster with environments" when {
    "there is no default at all" should {
      "be invalid" in {
        val kafkaCluster = KafkaCluster(
          KafkaClusterEntityId("test"),
          "broker1,broker2,broker3",
          environments = DeploymentEnvironments.OrderedVariables(
            DeploymentEnvironmentId("test") -> DeploymentEnvironment.Variables.empty
          )
        )
        val actualValidationResult = KafkaClusterValidator.validateKafkaCluster(KafkaClusterEntityId("test"), kafkaCluster)

        actualValidationResult should beInvalid
        actualValidationResult should containMessage("expecting the first deployment environment to be 'default' but was 'test'")
      }
    }

    "default is not the first one" should {
      "be invalid" in {
        val kafkaCluster = KafkaCluster(
          KafkaClusterEntityId("test"),
          "broker1,broker2,broker3",
          environments = DeploymentEnvironments.OrderedVariables(
            DeploymentEnvironmentId("test") -> DeploymentEnvironment.Variables.empty,
            DeploymentEnvironmentId("default") -> DeploymentEnvironment.Variables.empty
          )
        )
        val actualValidationResult = KafkaClusterValidator.validateKafkaCluster(KafkaClusterEntityId("test"), kafkaCluster)

        actualValidationResult should beInvalid
        actualValidationResult should containMessage("expecting the first deployment environment to be 'default' but was 'test'")
      }
    }

    "default is the only one" should {
      "be valid" in {
        val kafkaCluster = KafkaCluster(
          KafkaClusterEntityId("test"),
          "broker1,broker2,broker3",
          environments = DeploymentEnvironments.OrderedVariables(
            DeploymentEnvironmentId("default") -> DeploymentEnvironment.Variables.empty
          )
        )
        val actualValidationResult = KafkaClusterValidator.validateKafkaCluster(KafkaClusterEntityId("test"), kafkaCluster)

        actualValidationResult should beValid
      }
    }

    "default is the first one" should {
      "be valid" in {
        val kafkaCluster = KafkaCluster(
          KafkaClusterEntityId("test"),
          "broker1,broker2,broker3",
          environments = DeploymentEnvironments.OrderedVariables(
            DeploymentEnvironmentId("default") -> DeploymentEnvironment.Variables.empty,
            DeploymentEnvironmentId("test") -> DeploymentEnvironment.Variables.empty,
            DeploymentEnvironmentId("test-cluster") -> DeploymentEnvironment.Variables.empty
          )
        )
        val actualValidationResult = KafkaClusterValidator.validateKafkaCluster(KafkaClusterEntityId("test"), kafkaCluster)

        actualValidationResult should beValid
      }
    }
  }

  "KafkaCluster with empty replicaPlacements" when {
    "there is no default replicaPlacement" should {
      "be valid" in {
        val kafkaCluster = KafkaCluster(
          KafkaClusterEntityId("test"),
          "broker1,broker2,broker3",
          environments = DeploymentEnvironments.OrderedVariables(
            DeploymentEnvironmentId("default") -> DeploymentEnvironment.Variables.empty
          ),
          replicaPlacementConfigs = emptyReplicaPlacements,
          defaultReplicaPlacementId = None
        )
        val actualValidationResult = KafkaClusterValidator.validateKafkaCluster(KafkaClusterEntityId("test"), kafkaCluster)

        actualValidationResult should beValid
      }
    }

    "there is a default replicaPlacement" should {
      "be invalid" in {
        val kafkaCluster = KafkaCluster(
          KafkaClusterEntityId("test"),
          "broker1,broker2,broker3",
          environments = DeploymentEnvironments.OrderedVariables(
            DeploymentEnvironmentId("default") -> DeploymentEnvironment.Variables.empty
          ),
          replicaPlacementConfigs = emptyReplicaPlacements,
          defaultReplicaPlacementId = Some(ReplicaPlacementId("default"))
        )
        val actualValidationResult = KafkaClusterValidator.validateKafkaCluster(KafkaClusterEntityId("test"), kafkaCluster)

        actualValidationResult should beInvalid
        actualValidationResult should containMessage("the defaultReplicaPlacementId must not be set if the replicaPlacements is empty")
      }
    }
  }

  "KafkaCluster with some replicaPlacements" when {
    "there is no default replicaPlacement" should {
      "be invalid" in {
        val kafkaCluster = KafkaCluster(
          KafkaClusterEntityId("test"),
          "broker1,broker2,broker3",
          environments = DeploymentEnvironments.OrderedVariables(
            DeploymentEnvironmentId("default") -> DeploymentEnvironment.Variables.empty
          ),
          replicaPlacementConfigs = Map(ReplicaPlacementId("default") -> TopicConfigDefaults.fromReplicaPlacement("{}")),
          defaultReplicaPlacementId = None
        )
        val actualValidationResult = KafkaClusterValidator.validateKafkaCluster(KafkaClusterEntityId("test"), kafkaCluster)

        actualValidationResult should beInvalid
        actualValidationResult should containMessage("the defaultReplicaPlacementId must be set if the replicaPlacements is not empty")
      }
    }

    "there is an existing default replicaPlacement" should {
      "be valid" in {
        val kafkaCluster = KafkaCluster(
          KafkaClusterEntityId("test"),
          "broker1,broker2,broker3",
          environments = DeploymentEnvironments.OrderedVariables(
            DeploymentEnvironmentId("default") -> DeploymentEnvironment.Variables.empty
          ),
          replicaPlacementConfigs = Map(ReplicaPlacementId("default") -> TopicConfigDefaults.fromReplicaPlacement("{}")),
          defaultReplicaPlacementId = Some(ReplicaPlacementId("default"))
        )
        val actualValidationResult = KafkaClusterValidator.validateKafkaCluster(KafkaClusterEntityId("test"), kafkaCluster)

        actualValidationResult should beValid
      }
    }

    "there is non-existing default replicaPlacement" should {
      "be invalid" in {
        val kafkaCluster = KafkaCluster(
          KafkaClusterEntityId("test"),
          "broker1,broker2,broker3",
          environments = DeploymentEnvironments.OrderedVariables(
            DeploymentEnvironmentId("default") -> DeploymentEnvironment.Variables.empty
          ),
          replicaPlacementConfigs = Map(ReplicaPlacementId("default") -> TopicConfigDefaults.fromReplicaPlacement("{}")),
          defaultReplicaPlacementId = Some(ReplicaPlacementId("other"))
        )
        val actualValidationResult = KafkaClusterValidator.validateKafkaCluster(KafkaClusterEntityId("test"), kafkaCluster)

        actualValidationResult should beInvalid
        actualValidationResult should containMessage(s"the replicaPlacements doesn't have the defaultReplicaPlacementId = 'other'")
      }
    }

    "there is no default replicaPlacement with empty config name" should {
      "be invalid" in {
        val kafkaCluster = KafkaCluster(
          KafkaClusterEntityId("test"),
          "broker1,broker2,broker3",
          environments = DeploymentEnvironments.OrderedVariables(
            DeploymentEnvironmentId("default") -> DeploymentEnvironment.Variables.empty
          ),
          replicaPlacementConfigs = Map(ReplicaPlacementId("default") -> Map("" -> TopicConfigDefault(overridable = false, "abc".some))),
          defaultReplicaPlacementId = ReplicaPlacementId("default").some
        )
        val actualValidationResult = KafkaClusterValidator.validateKafkaCluster(KafkaClusterEntityId("test"), kafkaCluster)

        actualValidationResult should beInvalid
        actualValidationResult should containMessage("replicaPlacementConfig 'default' contains empty topic config key")
      }
    }
  }

  "there is non-existing system-topic replicaPlacement" should {
    "be invalid" in {
      val kafkaCluster = KafkaCluster(
        KafkaClusterEntityId("test"),
        "broker1,broker2,broker3",
        environments = DeploymentEnvironments.OrderedVariables(
          DeploymentEnvironmentId("default") -> DeploymentEnvironment.Variables.empty
        ),
        replicaPlacementConfigs = Map(ReplicaPlacementId("default") -> TopicConfigDefaults.fromReplicaPlacement("{}")),
        defaultReplicaPlacementId = Some(ReplicaPlacementId("default")),
        systemTopicsReplicaPlacementId = Some(ReplicaPlacementId("other"))
      )
      val actualValidationResult = KafkaClusterValidator.validateKafkaCluster(KafkaClusterEntityId("test"), kafkaCluster)

      actualValidationResult should beInvalid
      actualValidationResult should containMessage(s"the replicaPlacements doesn't have the systemTopicsReplicaPlacementId = 'other'")
    }
  }
}
