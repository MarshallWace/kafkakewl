/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import cats.syntax.option._
import com.mwam.kafkakewl.common.ValidationResultMatchers
import com.mwam.kafkakewl.domain.kafka.config.{TopicConfigDefault, TopicConfigDefaults, TopicConfigKeys}
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId, NonKewlKafkaResources, TopicConfigKeyConstraint, TopicConfigKeyConstraintInclusive}
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology.{Namespace, ReplicaPlacementId, TopologyEntityId, TopologyId, TopologyToDeploy}
import com.mwam.kafkakewl.domain.{TestTopologiesToDeploy, TestTopologiesToDeployCommon}
import org.scalatest.{FlatSpec, Matchers}

class TopologyToDeployValidatorStandaloneSpec extends FlatSpec
  with Matchers
  with ValidationResultMatchers
  with TestTopologiesToDeploy
  with TestTopologiesToDeployCommon {

  val kafkaCluster: KafkaCluster = KafkaCluster(
    KafkaClusterEntityId("test"),
    "broker1,broker2,broker3"
  )

  val topicDefaults: TopicDefaults = TopicDefaults()

  val defaultTopicConfigsForTopologies: TopicConfigKeyConstraintInclusive = TopicConfigKeyConstraintInclusive(
    include = Seq(
      TopicConfigKeyConstraint.Exact("cleanup.policy"),
      TopicConfigKeyConstraint.Exact("delete.retention.ms"),
      TopicConfigKeyConstraint.Exact("max.compaction.lag.ms"),
      TopicConfigKeyConstraint.Exact("min.cleanable.dirty.ratio"),
      TopicConfigKeyConstraint.Exact("min.compaction.lag.ms"),
      TopicConfigKeyConstraint.Exact("min.insync.replicas"),
      TopicConfigKeyConstraint.Exact("retention.ms"),
      TopicConfigKeyConstraint.Exact("segment.bytes"),
      TopicConfigKeyConstraint.Exact("segment.ms")
    )
  )

  "a topology with non-kewl topics" should "be invalid" in {
    val topologyToDeploy = TopologyToDeploy(topology = TopologyId("test"), topics = Map("connect-offsets" -> TopologyToDeploy.Topic("connect-offsets")).toMapByTopicId)
    val actualValidationResult = TopologyToDeployValidator.validateTopology(
      Map.empty,
      TopologyEntityId("test"),
      Some(topologyToDeploy),
      kafkaCluster.kafkaCluster,
      kafkaCluster.copy(nonKewl = NonKewlKafkaResources(topicRegexes = Seq("connect-.*"))),
      topicDefaults
    )
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("Topics 'connect-offsets' are non-kewl in the kafka cluster 'test'")
  }

  "a topology with non-kewl un-managed topics" should "be invalid" in {
    val topologyToDeploy = TopologyToDeploy(topology = TopologyId("test"), topics = Map("connect-offsets" -> TopologyToDeploy.Topic("connect-offsets", unManaged = true)).toMapByTopicId)
    val actualValidationResult = TopologyToDeployValidator.validateTopology(
      Map.empty,
      TopologyEntityId("test"),
      Some(topologyToDeploy),
      kafkaCluster.kafkaCluster,
      kafkaCluster.copy(nonKewl = NonKewlKafkaResources(topicRegexes = Seq("connect-.*"))),
      topicDefaults
    )
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("Topics 'connect-offsets' are non-kewl in the kafka cluster 'test'")
  }

  "a topology with a topic with an existing replicaPlacement" should "be valid" in {
    val topologyToDeploy = TopologyToDeploy(namespace = Namespace("test"), topics = Map("topic1" -> TopologyToDeploy.Topic("test.topic1", replicaPlacement = ReplicaPlacementId("something").some)).toMapByTopicId)
    val actualValidationResult = TopologyToDeployValidator.validateTopology(
      Map.empty,
      TopologyEntityId("test"),
      Some(topologyToDeploy),
      kafkaCluster.kafkaCluster,
      kafkaCluster.copy(replicaPlacementConfigs = Map(ReplicaPlacementId("something") -> TopicConfigDefaults.fromReplicaPlacement("{}"))),
      topicDefaults
    )
    actualValidationResult should beValid
  }

  "a topology with a topic with a replicaPlacement when the kafka-cluster doesn't have any" should "be invalid" in {
    val topologyToDeploy = TopologyToDeploy(namespace = Namespace("test"), topics = Map("topic1" -> TopologyToDeploy.Topic("test.topic1", replicaPlacement = ReplicaPlacementId("something").some)).toMapByTopicId)
    val actualValidationResult = TopologyToDeployValidator.validateTopology(
      Map.empty,
      TopologyEntityId("test"),
      Some(topologyToDeploy),
      kafkaCluster.kafkaCluster,
      kafkaCluster,
      topicDefaults
    )
    actualValidationResult should beInvalid
    actualValidationResult should containMessage(s"'replicaPlacement' = 'something' is invalid: you should ask the administrators to setup pre-defined replica-placements for the kafka-cluster")
  }

  "a topology with a topic with a replicaPlacement when the kafka-cluster has other ones" should "be invalid" in {
    val topologyToDeploy = TopologyToDeploy(namespace = Namespace("test"), topics = Map("topic1" -> TopologyToDeploy.Topic("test.topic1", replicaPlacement = ReplicaPlacementId("something").some)).toMapByTopicId)
    val actualValidationResult = TopologyToDeployValidator.validateTopology(
      Map.empty,
      TopologyEntityId("test"),
      Some(topologyToDeploy),
      kafkaCluster.kafkaCluster,
      kafkaCluster.copy(replicaPlacementConfigs = Map(ReplicaPlacementId("another") -> TopicConfigDefaults.fromReplicaPlacement("{}"))),
      topicDefaults
    )
    actualValidationResult should beInvalid
    actualValidationResult should containMessage(s"'replicaPlacement' = 'something' is invalid: possible pre-defined options: 'another'")
  }

  "a topology with a topic with replicationFactor = -1" should "be invalid" in {
    val topologyToDeploy = TopologyToDeploy(namespace = Namespace("test"), topics = Map("topic1" -> TopologyToDeploy.Topic("test.topic1", replicationFactor = (-1).toShort.some)).toMapByTopicId)
    val actualValidationResult = TopologyToDeployValidator.validateTopology(
      Map.empty,
      TopologyEntityId("test"),
      Some(topologyToDeploy),
      kafkaCluster.kafkaCluster,
      kafkaCluster,
      topicDefaults
    )
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("if 'replicationFactor' is set it must be positive but '-1' is not")
  }

  "a topology with a topic with replicationFactor = 0" should "be invalid" in {
    val topologyToDeploy = TopologyToDeploy(namespace = Namespace("test"), topics = Map("topic1" -> TopologyToDeploy.Topic("test.topic1", replicationFactor = 0.toShort.some)).toMapByTopicId)
    val actualValidationResult = TopologyToDeployValidator.validateTopology(
      Map.empty,
      TopologyEntityId("test"),
      Some(topologyToDeploy),
      kafkaCluster.kafkaCluster,
      kafkaCluster,
      topicDefaults
    )
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("if 'replicationFactor' is set it must be positive but '0' is not")
  }

  "a topology with a topic with replicationFactor = 1" should "be valid" in {
    val topologyToDeploy = TopologyToDeploy(namespace = Namespace("test"), topics = Map("topic1" -> TopologyToDeploy.Topic("test.topic1", replicationFactor = 1.toShort.some)).toMapByTopicId)
    val actualValidationResult = TopologyToDeployValidator.validateTopology(
      Map.empty,
      TopologyEntityId("test"),
      Some(topologyToDeploy),
      kafkaCluster.kafkaCluster,
      kafkaCluster,
      topicDefaults
    )
    actualValidationResult should beValid
  }

  "a topology with a topic with replicationFactor = 3 and replicaPlacement = 'default' when there is an empty replica-placement" should "be invalid" in {
    val topologyToDeploy = TopologyToDeploy(namespace = Namespace("test"), topics = Map("topic1" -> TopologyToDeploy.Topic("test.topic1", replicationFactor = 3.toShort.some, replicaPlacement = ReplicaPlacementId("default").some)).toMapByTopicId)
    val actualValidationResult = TopologyToDeployValidator.validateTopology(
      Map.empty,
      TopologyEntityId("test"),
      Some(topologyToDeploy),
      kafkaCluster.kafkaCluster,
      kafkaCluster.copy(replicaPlacementConfigs = Map(ReplicaPlacementId("default") -> TopicConfigDefaults.fromReplicaPlacement("{}"), ReplicaPlacementId("none") -> TopicConfigDefaults.noReplicaPlacement())),
      topicDefaults
    )
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("'replicationFactor' must not be set if there is a non-empty 'replicaPlacement' (= 'default'). Either remove the 'replicationFactor' property from topic 'test.topic1' or set the 'replicaPlacement' to one of the following: 'none'")
  }

  "a topology with a topic with replicationFactor = 3 and replicaPlacement = 'default' when there is no empty replica-placement" should "be invalid" in {
    val topologyToDeploy = TopologyToDeploy(namespace = Namespace("test"), topics = Map("topic1" -> TopologyToDeploy.Topic("test.topic1", replicationFactor = 3.toShort.some, replicaPlacement = ReplicaPlacementId("default").some)).toMapByTopicId)
    val actualValidationResult = TopologyToDeployValidator.validateTopology(
      Map.empty,
      TopologyEntityId("test"),
      Some(topologyToDeploy),
      kafkaCluster.kafkaCluster,
      kafkaCluster.copy(replicaPlacementConfigs = Map(ReplicaPlacementId("default") -> TopicConfigDefaults.fromReplicaPlacement("{}"))),
      topicDefaults
    )
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("'replicationFactor' must not be set if there is a non-empty 'replicaPlacement' (= 'default'). Remove the 'replicationFactor' property from topic 'test.topic1'")
  }

  "a topology with a topic with replicationFactor = 3 and replicaPlacement = 'none'" should "be valid" in {
    val topologyToDeploy = TopologyToDeploy(namespace = Namespace("test"), topics = Map("topic1" -> TopologyToDeploy.Topic("test.topic1", replicationFactor = 3.toShort.some, replicaPlacement = ReplicaPlacementId("none").some)).toMapByTopicId)
    val actualValidationResult = TopologyToDeployValidator.validateTopology(
      Map.empty,
      TopologyEntityId("test"),
      Some(topologyToDeploy),
      kafkaCluster.kafkaCluster,
      kafkaCluster.copy(replicaPlacementConfigs = Map(ReplicaPlacementId("default") -> TopicConfigDefaults.fromReplicaPlacement("{}"), ReplicaPlacementId("none") -> TopicConfigDefaults.noReplicaPlacement())),
      topicDefaults
    )
    actualValidationResult should beValid
  }

  "a topology with a topic with replicaPlacement = 'default' and config with min.insync.replicas set" should "be invalid" in {
    val topologyToDeploy = TopologyToDeploy(
      namespace = Namespace("test"),
      topics = Map(
        "topic1" -> TopologyToDeploy.Topic("test.topic1", replicaPlacement = ReplicaPlacementId("default").some, config = Map(TopicConfigKeys.minInsyncReplicas -> "1"))
      ).toMapByTopicId
    )
    val actualValidationResult = TopologyToDeployValidator.validateTopology(
      Map.empty,
      TopologyEntityId("test"),
      Some(topologyToDeploy),
      kafkaCluster.kafkaCluster,
      kafkaCluster.copy(replicaPlacementConfigs = Map(ReplicaPlacementId("default") -> (TopicConfigDefaults.fromReplicaPlacement("{}") + TopicConfigDefault.noMinInsyncReplicas()))),
      topicDefaults
    )
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("replicaPlacement = 'default' does not allow to set the 'min.insync.replicas' topic config. You can remove this topic config from your topic if you don't need it, set 'replicaPlacement' to another one that allows this config or sets it to value you need. Available replica-placements: 'default'")
  }

  "a topology with a topic with replicaPlacement = 'durable' and config with min.insync.replicas set" should "be invalid" in {
    val topologyToDeploy = TopologyToDeploy(
      namespace = Namespace("test"),
      topics = Map(
        "topic1" -> TopologyToDeploy.Topic("test.topic1", replicaPlacement = ReplicaPlacementId("durable").some, config = Map(TopicConfigKeys.minInsyncReplicas -> "1"))
      ).toMapByTopicId
    )
    val actualValidationResult = TopologyToDeployValidator.validateTopology(
      Map.empty,
      TopologyEntityId("test"),
      Some(topologyToDeploy),
      kafkaCluster.kafkaCluster,
      kafkaCluster.copy(replicaPlacementConfigs = Map(ReplicaPlacementId("durable") -> TopicConfigDefaults.fromReplicaPlacement("{}", minInsyncReplicas = 2))),
      topicDefaults
    )
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("replicaPlacement = 'durable' does not allow to set the 'min.insync.replicas' topic config. You can remove this topic config from your topic if you're happy with the replica-placement's default: '2', set 'replicaPlacement' to another one that allows this config or sets it to value you need. Available replica-placements: 'durable'")
  }

  "a topology with a topic with replicaPlacement = 'durable' and config with min.insync.replicas set to the same as in the replica-placement" should "be invalid" in {
    val topologyToDeploy = TopologyToDeploy(
      namespace = Namespace("test"),
      topics = Map(
        "topic1" -> TopologyToDeploy.Topic("test.topic1", replicaPlacement = ReplicaPlacementId("durable").some, config = Map(TopicConfigKeys.minInsyncReplicas -> "2"))
      ).toMapByTopicId
    )
    val actualValidationResult = TopologyToDeployValidator.validateTopology(
      Map.empty,
      TopologyEntityId("test"),
      Some(topologyToDeploy),
      kafkaCluster.kafkaCluster,
      kafkaCluster.copy(replicaPlacementConfigs = Map(ReplicaPlacementId("durable") -> TopicConfigDefaults.fromReplicaPlacement("{}", minInsyncReplicas = 2))),
      topicDefaults
    )
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("replicaPlacement = 'durable' does not allow to set the 'min.insync.replicas' topic config. You can remove this topic config from your topic because the replica-placement's default is the same as what you just set: '2'")
  }

  "a topology with a topic with configs when the allowed topic configs is empty" should "be valid" in {
    val topologyToDeploy = TopologyToDeploy(
      namespace = Namespace("test"),
      topics = Map(
        "topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map(TopicConfigKeys.minInsyncReplicas -> "2", TopicConfigKeys.confluentPlacementConstraints -> "{}", "blahblah" -> "abc"))
      ).toMapByTopicId
    )
    val actualValidationResult = TopologyToDeployValidator.validateTopology(
      Map.empty,
      TopologyEntityId("test"),
      Some(topologyToDeploy),
      kafkaCluster.kafkaCluster,
      kafkaCluster,
      topicDefaults
    )
    actualValidationResult should beValid
  }

  "a topology with a topic with configs that are allowed" should "be valid" in {
    val topologyToDeploy = TopologyToDeploy(
      namespace = Namespace("test"),
      topics = Map(
        "topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map(TopicConfigKeys.minInsyncReplicas -> "2", TopicConfigKeys.retentionMs -> "-1"))
      ).toMapByTopicId
    )
    val actualValidationResult = TopologyToDeployValidator.validateTopology(
      Map.empty,
      TopologyEntityId("test"),
      Some(topologyToDeploy),
      kafkaCluster.kafkaCluster,
      kafkaCluster.copy(topicConfigKeysAllowedInTopologies = defaultTopicConfigsForTopologies),
      topicDefaults
    )
    actualValidationResult should beValid
  }

  "a topology with a topic with configs that are not allowed" should "be invalid" in {
    val topologyToDeploy = TopologyToDeploy(
      namespace = Namespace("test"),
      topics = Map(
        "topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map(TopicConfigKeys.minInsyncReplicas -> "2", TopicConfigKeys.retentionMs -> "-1", TopicConfigKeys.confluentPlacementConstraints -> "{}", "blahblah" -> "abc"))
      ).toMapByTopicId
    )
    val actualValidationResult = TopologyToDeployValidator.validateTopology(
      Map.empty,
      TopologyEntityId("test"),
      Some(topologyToDeploy),
      kafkaCluster.kafkaCluster,
      kafkaCluster.copy(topicConfigKeysAllowedInTopologies = defaultTopicConfigsForTopologies),
      topicDefaults
    )
    actualValidationResult should beInvalid
    actualValidationResult should containMessages(
      "The 'confluent.placement.constraints' topic config is not allowed by the administrators. If you really need to use this topic-config ask the administrators to allow it in the kafka-cluster 'test'",
      "The 'blahblah' topic config is not allowed by the administrators. If you really need to use this topic-config ask the administrators to allow it in the kafka-cluster 'test'"
    )
  }

  "a topology with a topic with configs that are not allowed but there are replica-placements containing those" should "be invalid" in {
    val topologyToDeploy = TopologyToDeploy(
      namespace = Namespace("test"),
      topics = Map(
        "topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map(TopicConfigKeys.minInsyncReplicas -> "2", TopicConfigKeys.retentionMs -> "-1", TopicConfigKeys.confluentPlacementConstraints -> "{}"))
      ).toMapByTopicId
    )
    val actualValidationResult = TopologyToDeployValidator.validateTopology(
      Map.empty,
      TopologyEntityId("test"),
      Some(topologyToDeploy),
      kafkaCluster.kafkaCluster,
      kafkaCluster.copy(
        replicaPlacementConfigs = Map(
          ReplicaPlacementId("none") -> TopicConfigDefaults.noReplicaPlacement(),
          ReplicaPlacementId("durable") -> TopicConfigDefaults.fromReplicaPlacement("{}", minInsyncReplicas = 2)
        ),
        topicConfigKeysAllowedInTopologies = defaultTopicConfigsForTopologies
      ),
      topicDefaults
    )
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("The 'confluent.placement.constraints' topic config is not allowed by the administrators. If you really need to use this topic-config try setting the 'replicaPlacement' to one of the following: 'durable'")
  }
}
