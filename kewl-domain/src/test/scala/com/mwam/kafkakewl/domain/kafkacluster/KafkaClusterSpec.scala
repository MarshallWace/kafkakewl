/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain.kafkacluster

import cats.syntax.option._
import com.mwam.kafkakewl.domain.kafka.config.{TopicConfigDefault, TopicConfigDefaults, TopicConfigKeys}
import com.mwam.kafkakewl.domain.topology.ReplicaPlacementId
import org.scalatest.{FlatSpec, Matchers}

class KafkaClusterSpec extends FlatSpec with Matchers {
  val kafkaCluster: KafkaCluster = KafkaCluster(
    KafkaClusterEntityId("test"),
    "broker1,broker2,broker3"
  )

  // the actual jsons don't make sense, but they don't really matter
  val defaultReplicaPlacementJson = """{"durable": false}"""
  val durableReplicaPlacementJson = """{"durable": true}"""

  val defaultReplicaPlacementJsonKeyValue: (String, String) = (TopicConfigKeys.confluentPlacementConstraints -> defaultReplicaPlacementJson)
  val durableReplicaPlacementJsonKeyValue: (String, String) = (TopicConfigKeys.confluentPlacementConstraints -> durableReplicaPlacementJson)
  val retentionMsKeyValue: (String, String) = ("retention.ms" -> "2000000")
  val minInsyncReplicasTwoKeyValue: (String, String) = TopicConfigKeys.minInsyncReplicas -> "2"
  val minInsyncReplicasThreeKeyValue: (String, String) = TopicConfigKeys.minInsyncReplicas -> "3"

  val noneReplicaPlacement: Map[String, TopicConfigDefault] = TopicConfigDefaults.noReplicaPlacement()
  val defaultReplicaPlacement: Map[String, TopicConfigDefault] = TopicConfigDefaults.fromReplicaPlacement(defaultReplicaPlacementJson)
  val durableReplicaPlacement: Map[String, TopicConfigDefault] = TopicConfigDefaults.fromReplicaPlacement(durableReplicaPlacementJson, minInsyncReplicas = 2, minInsyncReplicasOverridable = false)
  val durableIsrReplicaPlacement: Map[String, TopicConfigDefault] = TopicConfigDefaults.fromReplicaPlacement(durableReplicaPlacementJson, minInsyncReplicas = 2, minInsyncReplicasOverridable = true)

  val kafkaClusterWithReplicaPlacements: KafkaCluster = kafkaCluster.copy(
    replicaPlacementConfigs = Map(
      ReplicaPlacementId("none") -> noneReplicaPlacement,
      ReplicaPlacementId("default") -> defaultReplicaPlacement,
      ReplicaPlacementId("durable") -> durableReplicaPlacement,
      ReplicaPlacementId("durable-isr") -> durableIsrReplicaPlacement,
    ),
    defaultReplicaPlacementId = ReplicaPlacementId("default").some
  )

  "when the kafka-cluster has no replica-placements to resolve" should "work" in {
    Seq(
      none,
      ReplicaPlacementId("").some,
      ReplicaPlacementId("none").some,
      ReplicaPlacementId("default").some,
      ReplicaPlacementId("durable").some,
      ReplicaPlacementId("durable-isr").some,
      ReplicaPlacementId("something-else").some
    ).foreach { replicaPlacementId =>
      // all replica-placement should resolve topic configs the same way
      kafkaCluster.resolveTopicConfig(replicaPlacementId, Map.empty) shouldBe Map.empty
      kafkaCluster.resolveTopicConfig(replicaPlacementId, Map(retentionMsKeyValue)) shouldBe Map(retentionMsKeyValue)
      kafkaCluster.resolveTopicConfig(replicaPlacementId, Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue)) shouldBe Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue)
      kafkaCluster.resolveTopicConfig(replicaPlacementId, Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue, minInsyncReplicasTwoKeyValue)) shouldBe Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue, minInsyncReplicasTwoKeyValue)
      kafkaCluster.resolveTopicConfig(replicaPlacementId, Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue, minInsyncReplicasThreeKeyValue)) shouldBe Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue, minInsyncReplicasThreeKeyValue)
    }
  }

  "when the kafka-cluster has some replica-placements to resolve" should "work" in {
    kafkaClusterWithReplicaPlacements.resolveTopicConfig(none, Map.empty) shouldBe Map(defaultReplicaPlacementJsonKeyValue)
    kafkaClusterWithReplicaPlacements.resolveTopicConfig(none, Map(retentionMsKeyValue)) shouldBe Map(defaultReplicaPlacementJsonKeyValue, retentionMsKeyValue)
    kafkaClusterWithReplicaPlacements.resolveTopicConfig(none, Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue)) shouldBe Map(defaultReplicaPlacementJsonKeyValue, retentionMsKeyValue)

    kafkaClusterWithReplicaPlacements.resolveTopicConfig(ReplicaPlacementId("").some, Map.empty) shouldBe Map.empty
    kafkaClusterWithReplicaPlacements.resolveTopicConfig(ReplicaPlacementId("").some, Map(retentionMsKeyValue)) shouldBe Map(retentionMsKeyValue)
    kafkaClusterWithReplicaPlacements.resolveTopicConfig(ReplicaPlacementId("").some, Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue)) shouldBe Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue)

    kafkaClusterWithReplicaPlacements.resolveTopicConfig(ReplicaPlacementId("none").some, Map.empty) shouldBe Map.empty
    kafkaClusterWithReplicaPlacements.resolveTopicConfig(ReplicaPlacementId("none").some, Map(retentionMsKeyValue)) shouldBe Map(retentionMsKeyValue)
    kafkaClusterWithReplicaPlacements.resolveTopicConfig(ReplicaPlacementId("none").some, Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue)) shouldBe Map(retentionMsKeyValue)

    kafkaClusterWithReplicaPlacements.resolveTopicConfig(ReplicaPlacementId("default").some, Map.empty) shouldBe Map(defaultReplicaPlacementJsonKeyValue)
    kafkaClusterWithReplicaPlacements.resolveTopicConfig(ReplicaPlacementId("default").some, Map(retentionMsKeyValue)) shouldBe Map(defaultReplicaPlacementJsonKeyValue, retentionMsKeyValue)
    kafkaClusterWithReplicaPlacements.resolveTopicConfig(ReplicaPlacementId("default").some, Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue, minInsyncReplicasThreeKeyValue)) shouldBe Map(defaultReplicaPlacementJsonKeyValue, minInsyncReplicasThreeKeyValue, retentionMsKeyValue)

    kafkaClusterWithReplicaPlacements.resolveTopicConfig(ReplicaPlacementId("durable").some, Map.empty) shouldBe Map(durableReplicaPlacementJsonKeyValue, minInsyncReplicasTwoKeyValue)
    kafkaClusterWithReplicaPlacements.resolveTopicConfig(ReplicaPlacementId("durable").some, Map(retentionMsKeyValue)) shouldBe Map(durableReplicaPlacementJsonKeyValue, minInsyncReplicasTwoKeyValue, retentionMsKeyValue)
    kafkaClusterWithReplicaPlacements.resolveTopicConfig(ReplicaPlacementId("durable").some, Map(retentionMsKeyValue, defaultReplicaPlacementJsonKeyValue, minInsyncReplicasThreeKeyValue)) shouldBe Map(durableReplicaPlacementJsonKeyValue, minInsyncReplicasTwoKeyValue, retentionMsKeyValue)

    kafkaClusterWithReplicaPlacements.resolveTopicConfig(ReplicaPlacementId("durable-isr").some, Map.empty) shouldBe Map(durableReplicaPlacementJsonKeyValue, minInsyncReplicasTwoKeyValue)
    kafkaClusterWithReplicaPlacements.resolveTopicConfig(ReplicaPlacementId("durable-isr").some, Map(retentionMsKeyValue)) shouldBe Map(durableReplicaPlacementJsonKeyValue, minInsyncReplicasTwoKeyValue, retentionMsKeyValue)
    kafkaClusterWithReplicaPlacements.resolveTopicConfig(ReplicaPlacementId("durable-isr").some, Map(retentionMsKeyValue, defaultReplicaPlacementJsonKeyValue, minInsyncReplicasThreeKeyValue)) shouldBe Map(durableReplicaPlacementJsonKeyValue, minInsyncReplicasThreeKeyValue, retentionMsKeyValue)

    kafkaClusterWithReplicaPlacements.resolveTopicConfig(ReplicaPlacementId("something-else").some, Map.empty) shouldBe Map.empty
    kafkaClusterWithReplicaPlacements.resolveTopicConfig(ReplicaPlacementId("something-else").some, Map(retentionMsKeyValue)) shouldBe Map(retentionMsKeyValue)
    kafkaClusterWithReplicaPlacements.resolveTopicConfig(ReplicaPlacementId("something-else").some, Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue)) shouldBe Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue)
  }

  "when the kafka-cluster has replica-placements to resolve and the default one is empty" should "" in {
    val kafkaClusterWithReplicaPlacementsDefaultNone = kafkaClusterWithReplicaPlacements.copy(defaultReplicaPlacementId = ReplicaPlacementId("none").some)
    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(none, Map.empty) shouldBe Map.empty
    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(none, Map(retentionMsKeyValue)) shouldBe Map(retentionMsKeyValue)
    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(none, Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue)) shouldBe Map(retentionMsKeyValue)

    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(ReplicaPlacementId("").some, Map.empty) shouldBe Map.empty
    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(ReplicaPlacementId("").some, Map(retentionMsKeyValue)) shouldBe Map(retentionMsKeyValue)
    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(ReplicaPlacementId("").some, Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue)) shouldBe Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue)

    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(ReplicaPlacementId("none").some, Map.empty) shouldBe Map.empty
    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(ReplicaPlacementId("none").some, Map(retentionMsKeyValue)) shouldBe Map(retentionMsKeyValue)
    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(ReplicaPlacementId("none").some, Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue)) shouldBe Map(retentionMsKeyValue)

    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(ReplicaPlacementId("default").some, Map.empty) shouldBe Map(defaultReplicaPlacementJsonKeyValue)
    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(ReplicaPlacementId("default").some, Map(retentionMsKeyValue)) shouldBe Map(defaultReplicaPlacementJsonKeyValue, retentionMsKeyValue)
    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(ReplicaPlacementId("default").some, Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue, minInsyncReplicasThreeKeyValue)) shouldBe Map(defaultReplicaPlacementJsonKeyValue, minInsyncReplicasThreeKeyValue, retentionMsKeyValue)

    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(ReplicaPlacementId("durable").some, Map.empty) shouldBe Map(durableReplicaPlacementJsonKeyValue, minInsyncReplicasTwoKeyValue)
    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(ReplicaPlacementId("durable").some, Map(retentionMsKeyValue)) shouldBe Map(durableReplicaPlacementJsonKeyValue, minInsyncReplicasTwoKeyValue, retentionMsKeyValue)
    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(ReplicaPlacementId("durable").some, Map(retentionMsKeyValue, defaultReplicaPlacementJsonKeyValue, minInsyncReplicasThreeKeyValue)) shouldBe Map(durableReplicaPlacementJsonKeyValue, minInsyncReplicasTwoKeyValue, retentionMsKeyValue)

    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(ReplicaPlacementId("durable-isr").some, Map.empty) shouldBe Map(durableReplicaPlacementJsonKeyValue, minInsyncReplicasTwoKeyValue)
    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(ReplicaPlacementId("durable-isr").some, Map(retentionMsKeyValue)) shouldBe Map(durableReplicaPlacementJsonKeyValue, minInsyncReplicasTwoKeyValue, retentionMsKeyValue)
    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(ReplicaPlacementId("durable-isr").some, Map(retentionMsKeyValue, defaultReplicaPlacementJsonKeyValue, minInsyncReplicasThreeKeyValue)) shouldBe Map(durableReplicaPlacementJsonKeyValue, minInsyncReplicasThreeKeyValue, retentionMsKeyValue)

    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(ReplicaPlacementId("something-else").some, Map.empty) shouldBe Map.empty
    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(ReplicaPlacementId("something-else").some, Map(retentionMsKeyValue)) shouldBe Map(retentionMsKeyValue)
    kafkaClusterWithReplicaPlacementsDefaultNone.resolveTopicConfig(ReplicaPlacementId("something-else").some, Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue)) shouldBe Map(retentionMsKeyValue, durableReplicaPlacementJsonKeyValue)
  }
}
