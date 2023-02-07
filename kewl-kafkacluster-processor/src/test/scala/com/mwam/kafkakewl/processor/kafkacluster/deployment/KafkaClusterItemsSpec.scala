/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.deployment

import cats.syntax.option._
import com.mwam.kafkakewl.domain.kafka.config.{TopicConfigDefaults, TopicConfigKeys}
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology._
import com.mwam.kafkakewl.domain.{FlexibleName, TestTopologiesToDeploy, TestTopologiesToDeployCommon}
import org.apache.kafka.common.acl.{AclOperation, AclPermissionType}
import org.apache.kafka.common.resource.{PatternType, ResourceType}
import org.scalatest.FlatSpec

class KafkaClusterItemsSpec extends FlatSpec with TestTopologiesToDeploy with TestTopologiesToDeployCommon {
  private def noCurrentTopologies: Map[TopologyEntityId, TopologyToDeploy] = Map.empty

  val kafkaCluster: KafkaCluster = KafkaCluster(
    KafkaClusterEntityId("test"),
    "broker1,broker2,broker3"
  )

  val topicDefaults: TopicDefaults = TopicDefaults()

  // the actual jsons don't make sense, but they don't really matter
  val noneReplicaPlacementJson = ""
  val defaultReplicaPlacementJson = """{"durable": false}"""
  val durableReplicaPlacementJson = """{"durable": true}"""

  val noneReplicaPlacement = TopicConfigDefaults.noReplicaPlacement()
  val defaultReplicaPlacement = TopicConfigDefaults.fromReplicaPlacement("""{"durable": false}""")
  val durableReplicaPlacement = TopicConfigDefaults.fromReplicaPlacement("""{"durable": true}""", minInsyncReplicas = 2)

  val kafkaClusterWithReplicaPlacements: KafkaCluster = kafkaCluster.copy(
    replicaPlacementConfigs = Map(
      ReplicaPlacementId("none") -> noneReplicaPlacement,
      ReplicaPlacementId("default") -> defaultReplicaPlacement,
      ReplicaPlacementId("durable") -> durableReplicaPlacement
    ),
    defaultReplicaPlacementId = ReplicaPlacementId("default").some
  )

  "an empty topology" should "generate no kafka cluster items" in {
    val kafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      noCurrentTopologies,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      TopologyToDeploy(),
      topicDefaults
    )
    assert(kafkaClusterItems.isEmpty)
  }

  "a simple standalone topology" should "generate the correct kafka cluster items" in {
    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test"),
        topics = Map(
          "topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000")),
          "topic2" -> TopologyToDeploy.Topic("test.topic2", config = Map("retention.ms" -> "31536000000"))
        ).toMapByTopicId,
        applications = Map(
          "processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"))
        ).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic1", None))), (RelationshipType.Produce(), Seq(("topic2", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      noCurrentTopologies,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Topic("test.topic2", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test")))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a simple standalone topology with a topic with replicaPlacement" should "generate the correct kafka cluster items" in {
    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test"),
        topics = Map(
          "topic1" -> TopologyToDeploy.Topic("test.topic1", replicationFactor = 3.toShort.some, replicaPlacement = ReplicaPlacementId("none").some, config = Map("retention.ms" -> "31536000000")),
          "topic2" -> TopologyToDeploy.Topic("test.topic2", replicaPlacement = ReplicaPlacementId("default").some, config = Map("retention.ms" -> "31536000000")),
          "topic3" -> TopologyToDeploy.Topic("test.topic3", replicaPlacement = ReplicaPlacementId("durable").some),
          "topic4" -> TopologyToDeploy.Topic("test.topic4"),
          "topic5" -> TopologyToDeploy.Topic("test.topic5", replicaPlacement = ReplicaPlacementId("none").some)
        ).toMapByTopicId,
        applications = Map(
          "processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"))
        ).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic1", None))), (RelationshipType.Produce(), Seq(("topic2", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaClusterWithReplicaPlacements.resolveTopicConfig,
      noCurrentTopologies,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Topic("test.topic2", 1, -1, config = Map("retention.ms" -> "31536000000", TopicConfigKeys.confluentPlacementConstraints -> defaultReplicaPlacementJson)).toTuple,
        KafkaClusterItem.Topic("test.topic3", 1, -1, config = Map(TopicConfigKeys.confluentPlacementConstraints -> durableReplicaPlacementJson, TopicConfigKeys.minInsyncReplicas -> "2")).toTuple,
        KafkaClusterItem.Topic("test.topic4", 1, -1, config = Map(TopicConfigKeys.confluentPlacementConstraints -> defaultReplicaPlacementJson)).toTuple,
        KafkaClusterItem.Topic("test.topic5", 1, 3, config = Map.empty).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test")))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a simple standalone topology with non-standard relationships" should "generate the correct kafka cluster items" in {
    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test"),
        topics = Map(
          "topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000")),
          "topic2" -> TopologyToDeploy.Topic("test.topic2", config = Map("retention.ms" -> "31536000000"))
        ).toMapByTopicId,
        applications = Map(
          "processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor")),
          "sink" -> TopologyToDeploy.Application("service-test-sink").makeSimple(Some("test.sink"))
        ).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor",
            (RelationshipType.Consume(), Seq(("topic1", None))),
            (RelationshipType.Produce(), Seq(("topic2", None))),
            // this should be ignored: custom relationships are irrelevant for kafka
            (RelationshipType.Custom("something-custom"), Seq(("processor", None)))
          ),
          // these should be ignored (only application-[consume/produce]-topic relationships are used to generate kafka items)
          toDeployRelationshipFrom("sink", (RelationshipType.Consume(), Seq(("processor", None)))),
          toDeployRelationshipFrom("topic1", (RelationshipType.Produce(), Seq(("processor", None)))),
          toDeployRelationshipFrom("topic2", (RelationshipType.Produce(), Seq(("topic1", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      noCurrentTopologies,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Topic("test.topic2", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test")))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a simple standalone topology with local regex relationships" should "generate the correct kafka cluster items" in {
    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test"),
        topics = Map(
          "topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000")),
          "topic2" -> TopologyToDeploy.Topic("test.topic2", config = Map("retention.ms" -> "31536000000")),
          "error" -> TopologyToDeploy.Topic("test.error", config = Map("retention.ms" -> "31536000000"))
        ).toMapByTopicId,
        applications = Map(
          "source" -> TopologyToDeploy.Application("service-test-source").makeSimple(),
          "processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor")),
          "sink" -> TopologyToDeploy.Application("service-test-sink").makeSimple(Some("test.sink"))
        ).toMapByApplicationId,
        aliases = LocalAliases(
          topics = Map(LocalAliasId("topics") -> Seq(FlexibleNodeId.Regex("""test\.topic\d"""))),
          applications = Map(LocalAliasId("applications") -> Seq(FlexibleNodeId.Regex(""".*""")))
        ),
        relationships = Map(
          toDeployRelationshipFrom("source", (RelationshipType.Produce(), Seq(("topics", None)))),
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic1", None))), (RelationshipType.Produce(), Seq(("topic2", None)))),
          toDeployRelationshipFrom("sink", (RelationshipType.Consume(), Seq(("topic2", None)))),
          toDeployRelationshipFrom("applications", (RelationshipType.Produce(), Seq(("error", None)))),
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      noCurrentTopologies,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Topic("test.topic2", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Topic("test.error", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-source", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-sink", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-source", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-source", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-source", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-source", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.error", "User:service-test-source", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.error", "User:service-test-source", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.error", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.error", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.sink", "User:service-test-sink", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-sink", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-sink", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.error", "User:service-test-sink", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.error", "User:service-test-sink", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test")))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a simple standalone topology with an un-managed topic" should "generate the correct kafka cluster items" in {
    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test"),
        topics = Map(
          "topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000")),
          "topic2" -> TopologyToDeploy.Topic("test.topic2", unManaged = true, config = Map("retention.ms" -> "31536000000"))
        ).toMapByTopicId,
        applications = Map(
          "processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"))
        ).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic1", None))), (RelationshipType.Produce(), Seq(("topic2", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      noCurrentTopologies,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Topic("test.topic2", 1, 3, config = Map("retention.ms" -> "31536000000"), isReal = false).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test")))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a simple standalone topology with a transactional id" should "generate the correct kafka cluster items" in {
    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test"),
        topics = Map(
          "topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000")),
          "topic2" -> TopologyToDeploy.Topic("test.topic2", config = Map("retention.ms" -> "31536000000"))
        ).toMapByTopicId,
        applications = Map(
          "processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"), Some("test.processor.transactional.id"))
        ).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic1", None))), (RelationshipType.Produce(), Seq(("topic2", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      noCurrentTopologies,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Topic("test.topic2", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TRANSACTIONAL_ID, PatternType.PREFIXED, "test.processor.transactional.id", "User:service-test-processor", "*", AclOperation.ALL, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test")))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a simple standalone topology with security disabled" should "generate the correct kafka cluster items" in {
    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test"),
        topics = Map(
          "topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000")),
          "topic2" -> TopologyToDeploy.Topic("test.topic2", config = Map("retention.ms" -> "31536000000"))
        ).toMapByTopicId,
        applications = Map(
          "processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"))
        ).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic1", None))), (RelationshipType.Produce(), Seq(("topic2", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      noCurrentTopologies,
      isKafkaClusterSecurityEnabled = false,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Topic("test.topic2", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test")))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a simple standalone topology with the same app producing/consuming" should "generate the correct kafka cluster items" in {
    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test"),
        topics = Map("topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000"))).toMapByTopicId,
        applications = Map("processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"))).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic1", None))), (RelationshipType.Produce(), Seq(("topic1", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      noCurrentTopologies,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test")))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a simple standalone topology with empty namespace and with developers (full access)" should "generate the correct kafka cluster items" in {
    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace(""),
        TopologyId("test"),
        developers = Seq("developer1"),
        developersAccess = DevelopersAccess.Full,
        topics = Map("topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000"))).toMapByTopicId,
        applications = Map("processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"))).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic1", None))), (RelationshipType.Produce(), Seq(("topic1", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      noCurrentTopologies,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:developer1", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:developer1", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.PREFIXED, "user.developer1", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test")))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a simple standalone topology with empty namespace and with developers (topic-readonly access)" should "generate the correct kafka cluster items" in {
    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace(""),
        TopologyId("test"),
        developers = Seq("developer1"),
        developersAccess = DevelopersAccess.TopicReadOnly,
        topics = Map("topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000"))).toMapByTopicId,
        applications = Map("processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"))).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic1", None))), (RelationshipType.Produce(), Seq(("topic1", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      noCurrentTopologies,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:developer1", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:developer1", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.PREFIXED, "user.developer1", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test")))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a simple standalone topology with developers (full access)" should "generate the correct kafka cluster items" in {
    val topology: TopologyToDeploy =
      TopologyToDeploy(
        namespace = Namespace("test"),
        developers = Seq("developer1"),
        developersAccess = DevelopersAccess.Full,
        topics = Map("topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000"))).toMapByTopicId,
        applications = Map("processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"))).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic1", None))), (RelationshipType.Produce(), Seq(("topic1", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      noCurrentTopologies,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:developer1", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.PREFIXED, "user.developer1", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        // because the namespace is NOT empty, we can have PREFIXED "test" acls
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TRANSACTIONAL_ID, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.ALL, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.CREATE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.DELETE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test")))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a simple standalone topology with developers (topic-readonly access)" should "generate the correct kafka cluster items" in {
    val topology: TopologyToDeploy =
      TopologyToDeploy(
        namespace = Namespace("test"),
        developers = Seq("developer1"),
        developersAccess = DevelopersAccess.TopicReadOnly,
        topics = Map("topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000"))).toMapByTopicId,
        applications = Map("processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"))).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic1", None))), (RelationshipType.Produce(), Seq(("topic1", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      noCurrentTopologies,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.PREFIXED, "user.developer1", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        // because the namespace is NOT empty, we can have PREFIXED "test" acls
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test")))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a topology referencing external topics in the same namespace with developers (full access)" should "generate the correct kafka cluster items" in {
    val sharedTopology: TopologyToDeploy =
      TopologyToDeploy(
        namespace = Namespace("test"),
        TopologyId("shared"),
        topics = Map(
          "topic-to-consume" -> TopologyToDeploy.Topic("test.shared.topic-to-consume", otherConsumerNamespaces = Seq(FlexibleName.Any())),
          "topic-to-consume-produce" -> TopologyToDeploy.Topic("test.shared.topic-to-consume-produce", otherConsumerNamespaces = Seq(FlexibleName.Any()), otherProducerNamespaces = Seq(FlexibleName.Any())),
          "topic-to-produce" -> TopologyToDeploy.Topic("test.shared.topic-to-produce", otherProducerNamespaces = Seq(FlexibleName.Any()))
        ).toMapByTopicId)

    val topology: TopologyToDeploy =
      TopologyToDeploy(
        namespace = Namespace("test"),
        developers = Seq("developer1"),
        developersAccess = DevelopersAccess.Full,
        topics = Map("topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000"))).toMapByTopicId,
        applications = Map("processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"))).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic1", None), ("test.shared.topic-to-consume", None), ("test.shared.topic-to-consume-produce", None))), (RelationshipType.Produce(), Seq(("topic1", None), ("test.shared.topic-to-produce", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      Seq(sharedTopology, topology).toMapByTopologyId,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.shared.topic-to-consume", config = Map.empty).toTuple,
        KafkaClusterItem.Topic("test.shared.topic-to-consume-produce", config = Map.empty).toTuple,
        KafkaClusterItem.Topic("test.shared.topic-to-produce", config = Map.empty).toTuple,
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test.shared"))) ++
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:developer1", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.PREFIXED, "user.developer1", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TRANSACTIONAL_ID, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.ALL, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.CREATE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.DELETE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test"))) ++
    Map(
      KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
      KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic-to-consume", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
      KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic-to-consume", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
      KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic-to-consume-produce", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
      KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic-to-consume-produce", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
      KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic-to-produce", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
      KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic-to-produce", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
    ).mapValues(_.withOwnerTopologyIds(Set(TopologyEntityId("test"), TopologyEntityId("test.shared"))))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a topology referencing external topics in the same namespace with developers (topic-readonly access)" should "generate the correct kafka cluster items" in {
    val sharedTopology: TopologyToDeploy =
      TopologyToDeploy(
        namespace = Namespace("test.shared"),
        topics = Map(
          "topic-to-consume" -> TopologyToDeploy.Topic("test.shared.topic-to-consume", otherConsumerNamespaces = Seq(FlexibleName.Any())),
          "topic-to-consume-produce" -> TopologyToDeploy.Topic("test.shared.topic-to-consume-produce", otherConsumerNamespaces = Seq(FlexibleName.Any()), otherProducerNamespaces = Seq(FlexibleName.Any())),
          "topic-to-produce" -> TopologyToDeploy.Topic("test.shared.topic-to-produce", otherProducerNamespaces = Seq(FlexibleName.Any()))
        ).toMapByTopicId)

    val topology: TopologyToDeploy =
      TopologyToDeploy(
        namespace = Namespace("test"),
        developers = Seq("developer1"),
        developersAccess = DevelopersAccess.TopicReadOnly,
        topics = Map("topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000"))).toMapByTopicId,
        applications = Map("processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"))).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic1", None), ("test.shared.topic-to-consume", None), ("test.shared.topic-to-consume-produce", None))), (RelationshipType.Produce(), Seq(("topic1", None), ("test.shared.topic-to-produce", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      Seq(sharedTopology, topology).toMapByTopologyId,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.shared.topic-to-consume", config = Map.empty).toTuple,
        KafkaClusterItem.Topic("test.shared.topic-to-consume-produce", config = Map.empty).toTuple,
        KafkaClusterItem.Topic("test.shared.topic-to-produce", config = Map.empty).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test.shared"))) ++
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.PREFIXED, "user.developer1", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test"))) ++
      Map(
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic-to-consume", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic-to-consume", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic-to-consume-produce", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic-to-consume-produce", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic-to-produce", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic-to-produce", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyIds(Set(TopologyEntityId("test"), TopologyEntityId("test.shared"))))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a topology referencing external topics with developers (full access)" should "generate the correct kafka cluster items" in {
    val sharedTopology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("shared"),
        topics = Map(
          "topic-to-consume" -> TopologyToDeploy.Topic("shared.topic-to-consume", otherConsumerNamespaces = Seq(FlexibleName.Any())),
          "topic-to-consume-produce" -> TopologyToDeploy.Topic("shared.topic-to-consume-produce", otherConsumerNamespaces = Seq(FlexibleName.Any()), otherProducerNamespaces = Seq(FlexibleName.Any())),
          "topic-to-produce" -> TopologyToDeploy.Topic("shared.topic-to-produce", otherProducerNamespaces = Seq(FlexibleName.Any()))
        ).toMapByTopicId
      )

    val topology: TopologyToDeploy =
      TopologyToDeploy(
        namespace = Namespace("test"),
        developers = Seq("developer1"),
        developersAccess = DevelopersAccess.Full,
        topics = Map("topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000"))).toMapByTopicId,
        applications = Map("processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"))).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic1", None), ("shared.topic-to-consume", None), ("shared.topic-to-consume-produce", None))), (RelationshipType.Produce(), Seq(("topic1", None), ("shared.topic-to-produce", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      Seq(sharedTopology, topology).toMapByTopologyId,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("shared.topic-to-consume", config = Map.empty).toTuple,
        KafkaClusterItem.Topic("shared.topic-to-consume-produce", config = Map.empty).toTuple,
        KafkaClusterItem.Topic("shared.topic-to-produce", config = Map.empty).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("shared"))) ++
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:developer1", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.PREFIXED, "user.developer1", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TRANSACTIONAL_ID, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.ALL, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.CREATE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.DELETE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume-produce", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume-produce", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume-produce", "User:developer1", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-produce", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-produce", "User:developer1", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test"))) ++
      Map(
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume-produce", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume-produce", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-produce", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-produce", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyIds(Set(TopologyEntityId("test"), TopologyEntityId("shared"))))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a topology referencing external topics with developers (topic read-only access)" should "generate the correct kafka cluster items" in {
    val sharedTopology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("shared"),
        topics = Map(
          "topic-to-consume" -> TopologyToDeploy.Topic("shared.topic-to-consume", otherConsumerNamespaces = Seq(FlexibleName.Any())),
          "topic-to-consume-produce" -> TopologyToDeploy.Topic("shared.topic-to-consume-produce", otherConsumerNamespaces = Seq(FlexibleName.Any()), otherProducerNamespaces = Seq(FlexibleName.Any())),
          "topic-to-produce" -> TopologyToDeploy.Topic("shared.topic-to-produce", otherProducerNamespaces = Seq(FlexibleName.Any()))
        ).toMapByTopicId
      )

    val topology: TopologyToDeploy =
      TopologyToDeploy(
        namespace = Namespace("test"),
        developers = Seq("developer1"),
        developersAccess = DevelopersAccess.TopicReadOnly,
        topics = Map("topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000"))).toMapByTopicId,
        applications = Map("processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"))).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic1", None), ("shared.topic-to-consume", None), ("shared.topic-to-consume-produce", None))), (RelationshipType.Produce(), Seq(("topic1", None), ("shared.topic-to-produce", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      Seq(sharedTopology, topology).toMapByTopologyId,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("shared.topic-to-consume", config = Map.empty).toTuple,
        KafkaClusterItem.Topic("shared.topic-to-consume-produce", config = Map.empty).toTuple,
        KafkaClusterItem.Topic("shared.topic-to-produce", config = Map.empty).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("shared"))) ++
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.PREFIXED, "user.developer1", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume-produce", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume-produce", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-produce", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test"))) ++
      Map(
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume-produce", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume-produce", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-produce", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-produce", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyIds(Set(TopologyEntityId("test"), TopologyEntityId("shared"))))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a topology in a topic-default consumer namespace referencing external topics with developers (topic read-only access)" should "generate the correct kafka cluster items" in {
    val sharedTopology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("shared"),
        topics = Map(
          "topic-to-consume" -> TopologyToDeploy.Topic("shared.topic-to-consume"),
          "topic-to-consume-produce" -> TopologyToDeploy.Topic("shared.topic-to-consume-produce"),
          "topic-to-produce" -> TopologyToDeploy.Topic("shared.topic-to-produce")
        ).toMapByTopicId
      )

    val topology: TopologyToDeploy =
      TopologyToDeploy(
        namespace = Namespace("test"),
        developers = Seq("developer1"),
        developersAccess = DevelopersAccess.TopicReadOnly,
        topics = Map("topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000"))).toMapByTopicId,
        applications = Map("processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"))).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic1", None), ("shared.topic-to-consume", None), ("shared.topic-to-consume-produce", None))), (RelationshipType.Produce(), Seq(("topic1", None), ("shared.topic-to-produce", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      Seq(sharedTopology, topology).toMapByTopologyId,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults.withConsumerNamespace("test").withProducerNamespace("test")
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("shared.topic-to-consume", config = Map.empty).toTuple,
        KafkaClusterItem.Topic("shared.topic-to-consume-produce", config = Map.empty).toTuple,
        KafkaClusterItem.Topic("shared.topic-to-produce", config = Map.empty).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("shared"))) ++
        Map(
          KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
          KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
          KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
          KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
          KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
          KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.PREFIXED, "user.developer1", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
          KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
          KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
          KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
          KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
          KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume-produce", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
          KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume-produce", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
          KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-produce", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
          KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-produce", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test"))) ++
        Map(
          KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
          KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
          KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
          KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume-produce", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
          KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume-produce", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
          KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-produce", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
          KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-produce", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
        ).mapValues(_.withOwnerTopologyIds(Set(TopologyEntityId("test"), TopologyEntityId("shared"))))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a simple standalone topology with custom consumer group names and transactional id" should "generate the correct kafka cluster items" in {
    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test"),
        topics = Map(
          "topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000")),
          "topic2" -> TopologyToDeploy.Topic("test.topic2", config = Map("retention.ms" -> "31536000000"))
        ).toMapByTopicId,
        applications = Map(
          "processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor-consumer-group"), Some("test.processor-transactional-id"))
        ).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic1", None))), (RelationshipType.Produce(), Seq(("topic2", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      noCurrentTopologies,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Topic("test.topic2", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor-consumer-group", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TRANSACTIONAL_ID, PatternType.PREFIXED, "test.processor-transactional-id", "User:service-test-processor", "*", AclOperation.ALL, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test")))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a simple standalone topology with kafka streams" should "generate the correct kafka cluster items" in {
    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test"),
        topics = Map(
          "topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000")),
          "topic2" -> TopologyToDeploy.Topic("test.topic2", config = Map("retention.ms" -> "31536000000"))
        ).toMapByTopicId,
        applications = Map(
          "processor" -> TopologyToDeploy.Application("service-test-processor").makeKafkaStreams("test.__kstreams__.MainProcessor")
        ).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic1", None))), (RelationshipType.Produce(), Seq(("topic2", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      noCurrentTopologies,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Topic("test.topic2", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "*", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.__kstreams__.MainProcessor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TRANSACTIONAL_ID, PatternType.PREFIXED, "test.__kstreams__.MainProcessor", "User:service-test-processor", "*", AclOperation.ALL, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test.__kstreams__.MainProcessor", "User:service-test-processor", "*", AclOperation.CREATE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test.__kstreams__.MainProcessor", "User:service-test-processor", "*", AclOperation.DELETE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test.__kstreams__.MainProcessor", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test.__kstreams__.MainProcessor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test.__kstreams__.MainProcessor", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test")))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a simple standalone topology with kafka streams, multiple applications" should "generate the correct kafka cluster items" in {
    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test"),
        topics = Map(
          "topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000")),
          "topic2" -> TopologyToDeploy.Topic("test.topic2", config = Map("retention.ms" -> "31536000000"))
        ).toMapByTopicId,
        applications = Map(
          "processor-Parser" -> TopologyToDeploy.Application("service-test-processor").makeKafkaStreams("test.__kstreams__.MainProcessor"),
          "processor-Enricher" -> TopologyToDeploy.Application("service-test-processor").makeKafkaStreams("test.__kstreams__.MainProcessor")
        ).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor-Parser", (RelationshipType.Consume(), Seq(("topic1", None))), (RelationshipType.Produce(), Seq(("processor-Enricher", None)))),
          toDeployRelationshipFrom("processor-Enricher", (RelationshipType.Produce(), Seq(("topic2", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      noCurrentTopologies,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Topic("test.topic2", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "*", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.__kstreams__.MainProcessor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TRANSACTIONAL_ID, PatternType.PREFIXED, "test.__kstreams__.MainProcessor", "User:service-test-processor", "*", AclOperation.ALL, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test.__kstreams__.MainProcessor", "User:service-test-processor", "*", AclOperation.CREATE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test.__kstreams__.MainProcessor", "User:service-test-processor", "*", AclOperation.DELETE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test.__kstreams__.MainProcessor", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test.__kstreams__.MainProcessor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test.__kstreams__.MainProcessor", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test")))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "two simple topologies referencing each other via a shared topic to consume" should "generate the correct kafka cluster items" in {
    val sharedTopology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test.shared"),
        topics = Map(
          "topic" -> TopologyToDeploy.Topic("test.shared.topic", config = Map("retention.ms" -> "31536000000"), otherConsumerNamespaces = Seq(FlexibleName.Any()))
        ).toMapByTopicId,
        applications = Map(
          "producer" -> TopologyToDeploy.Application("service-test-shared-producer")
        ).toMapByApplicationId,
        relationships = Map(toDeployRelationshipFrom("producer", (RelationshipType.Produce(), Seq(("topic", None))))
        )
      )

    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test"),
        topics = Map(
          "shared.derived.topic" -> TopologyToDeploy.Topic("test.shared.derived.topic", config = Map("retention.ms" -> "31536000000"))
        ).toMapByTopicId,
        applications = Map(
          "processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"))
        ).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("test.shared.topic", None))), (RelationshipType.Produce(), Seq(("shared.derived.topic", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      Seq(sharedTopology, topology).toMapByTopologyId,
      isKafkaClusterSecurityEnabled = true,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.shared.topic", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-shared-producer", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic", "User:service-test-shared-producer", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic", "User:service-test-shared-producer", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test.shared"))) ++
      Map(
        KafkaClusterItem.Topic("test.shared.derived.topic", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-processor", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.derived.topic", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.derived.topic", "User:service-test-processor", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test"))) ++
      Map(
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyIds(Set(TopologyEntityId("test.shared"), TopologyEntityId("test"))))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "two simple topologies referencing each other via a shared topic to consume with security disabled" should "generate the correct kafka cluster items" in {
    val sharedTopology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test.shared"),
        topics = Map(
          "topic" -> TopologyToDeploy.Topic("test.shared.topic", config = Map("retention.ms" -> "31536000000"), otherConsumerNamespaces = Seq(FlexibleName.Any()))
        ).toMapByTopicId,
        applications = Map(
          "producer" -> TopologyToDeploy.Application("service-test-shared-producer")
        ).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("producer", (RelationshipType.Produce(), Seq(("topic", None)))),
        )
      )

    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test"),
        topics = Map(
          "shared.derived.topic" -> TopologyToDeploy.Topic("test.shared.derived.topic", config = Map("retention.ms" -> "31536000000"))
        ).toMapByTopicId,
        applications = Map(
          "processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"))
        ).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("test.shared.topic", None))), (RelationshipType.Produce(), Seq(("shared.derived.topic", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      Seq(sharedTopology, topology).toMapByTopologyId,
      isKafkaClusterSecurityEnabled = false,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.shared.topic", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test.shared"))) ++
      Map(
        KafkaClusterItem.Topic("test.shared.derived.topic", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test")))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "two simple topologies referencing each other via a shared topic to produce" should "generate the correct kafka cluster items" in {
    val sharedTopology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test.shared"),
        topics = Map(
          "topic" -> TopologyToDeploy.Topic("test.shared.topic", config = Map("retention.ms" -> "31536000000"), otherProducerNamespaces = Seq(FlexibleName.Any()))
        ).toMapByTopicId,
        applications = Map(
          "consumer" -> TopologyToDeploy.Application("service-test-shared-consumer").makeSimple(Some("test.shared.consumer"))
        ).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("consumer", (RelationshipType.Consume(), Seq(("topic", None)))),
        )
      )

    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test"),
        applications = Map(
          "producer" -> TopologyToDeploy.Application("service-test-producer")
        ).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("producer", (RelationshipType.Produce(), Seq(("test.shared.topic", None)))),
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      Seq(sharedTopology, topology).toMapByTopologyId,
      isKafkaClusterSecurityEnabled = true,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.shared.topic", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.shared.consumer", "User:service-test-shared-consumer", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic", "User:service-test-shared-consumer", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic", "User:service-test-shared-consumer", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test.shared"))) ++
      Map(
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-producer", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic", "User:service-test-producer", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic", "User:service-test-producer", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyIds(Set(TopologyEntityId("test.shared"), TopologyEntityId("test"))))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "two simple topologies referencing each other via a shared consuming application" should "generate the correct kafka cluster items" in {
    val sharedTopology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test.shared"),
        applications = Map(
          "processor" -> TopologyToDeploy.Application("service-test-shared-processor", otherConsumableNamespaces = Seq(FlexibleName.Any())).makeSimple(Some("test.shared.processor"))
        ).toMapByApplicationId
      )

    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test"),
        topics = Map(
          "topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000"))
        ).toMapByTopicId,
        applications = Map(
          "producer" -> TopologyToDeploy.Application("service-test-producer")
        ).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("test.shared.processor", (RelationshipType.Consume(), Seq(("topic1", None)))),
          toDeployRelationshipFrom("producer", (RelationshipType.Produce(), Seq(("topic1", None)))),
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      Seq(sharedTopology, topology).toMapByTopologyId,
      isKafkaClusterSecurityEnabled = true,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.shared.processor", "User:service-test-shared-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test.shared"))) ++
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-producer", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-producer", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-producer", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test"))) ++
      Map(
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-shared-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-shared-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyIds(Set(TopologyEntityId("test.shared"), TopologyEntityId("test"))))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "two simple topologies referencing each other via a shared producing application" should "generate the correct kafka cluster items" in {
    val sharedTopology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test.shared"),
        applications = Map(
          "producer" -> TopologyToDeploy.Application("service-test-shared-producer", otherProducableNamespaces = Seq(FlexibleName.Any()))
        ).toMapByApplicationId
      )

    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test"),
        topics = Map(
          "topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000"))
        ).toMapByTopicId,
        applications = Map(
          "processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"))
        ).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("test.shared.producer", (RelationshipType.Produce(), Seq(("topic1", None)))),
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic1", None)))),
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      Seq(sharedTopology, topology).toMapByTopologyId,
      isKafkaClusterSecurityEnabled = true,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test"))) ++
      Map(
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-shared-producer", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-shared-producer", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-shared-producer", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyIds(Set(TopologyEntityId("test.shared"), TopologyEntityId("test"))))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "two simple independent topologies' consuming the same shared one" should "generate the correct kafka cluster items" in {
    val sharedTopology = topologyToDeploySharedConsumableTopic()
    val topology1 = topologyToDeployConsumingSharedTopic("test1", user = Some("service-test"))
    val topology2 = topologyToDeployConsumingSharedTopic("test2", user = Some("service-test"))

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      Seq(sharedTopology, topology1, topology2).toMapByTopologyId,
      isKafkaClusterSecurityEnabled = true,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("shared.topic-to-consume", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-shared", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume", "User:service-shared", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume", "User:service-shared", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("shared"))) ++
      Map(
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test1.sink", "User:service-test", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test1"))) ++
      Map(
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test2.sink", "User:service-test", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test2"))) ++
      Map(
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume", "User:service-test", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "shared.topic-to-consume", "User:service-test", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyIds(Set(TopologyEntityId("shared"), TopologyEntityId("test1"), TopologyEntityId("test2"))))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "two simple independent topologies' with the same developers" should "generate the correct kafka cluster items" in {
    val developers = Seq("developer1")
    val topology1 = topologyToDeploySourceSink("test1", developersAccess = DevelopersAccess.TopicReadOnly)
      .copy(namespace = Namespace("test1"), developers = developers)
    val topology2 = topologyToDeploySourceSink("test2", developersAccess = DevelopersAccess.Full)
      .copy(namespace = Namespace("test2"), developers = developers)

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      Seq(topology1, topology2).toMapByTopologyId,
      isKafkaClusterSecurityEnabled = true,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.PREFIXED, "user.developer1", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyIds(Set(TopologyEntityId("test1"), TopologyEntityId("test2")))) ++
      Map(
        KafkaClusterItem.Topic("test1.topic", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test1", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test1.sink", "User:service-test1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TRANSACTIONAL_ID, PatternType.PREFIXED, "test1.sink", "User:service-test1", "*", AclOperation.ALL, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test1.topic", "User:service-test1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test1.topic", "User:service-test1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test1.topic", "User:service-test1", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test1", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test1", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test1"))) ++
      Map(
        KafkaClusterItem.Topic("test2.topic", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test2", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test2.sink", "User:service-test2", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TRANSACTIONAL_ID, PatternType.PREFIXED, "test2.sink", "User:service-test2", "*", AclOperation.ALL, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test2.topic", "User:service-test2", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test2.topic", "User:service-test2", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test2.topic", "User:service-test2", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:developer1", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test2", "User:developer1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test2", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test2", "User:developer1", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.PREFIXED, "test2", "User:developer1", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TRANSACTIONAL_ID, PatternType.PREFIXED, "test2", "User:developer1", "*", AclOperation.ALL, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test2", "User:developer1", "*", AclOperation.CREATE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "test2", "User:developer1", "*", AclOperation.DELETE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test2")))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "two simple topologies referencing each other via a shared topic to consume with regex relationships" should "generate the correct kafka cluster items" in {
    val sharedTopology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test.shared"),
        topics = Map(
          "topic1" -> TopologyToDeploy.Topic("test.shared.topic1", config = Map("retention.ms" -> "31536000000"), otherConsumerNamespaces = Seq(FlexibleName.Any())),
          "topic2" -> TopologyToDeploy.Topic("test.shared.topic2", config = Map("retention.ms" -> "31536000000"), otherConsumerNamespaces = Seq(FlexibleName.Any()))
        ).toMapByTopicId,
        applications = Map(
          "producer" -> TopologyToDeploy.Application("service-test-shared-producer")
        ).toMapByApplicationId,
        aliases = LocalAliases(
          topics = Map(LocalAliasId("topics") -> Seq(FlexibleNodeId.Regex(""".*""")))
        ),
        relationships = Map(toDeployRelationshipFrom("producer", (RelationshipType.Produce(), Seq(("topics", None)))))
      )

    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test"),
        topics = Map(
          "topic1" -> TopologyToDeploy.Topic("test.topic1", config = Map("retention.ms" -> "31536000000")),
          "topic2" -> TopologyToDeploy.Topic("test.topic2", config = Map("retention.ms" -> "31536000000"))
        ).toMapByTopicId,
        applications = Map(
          "source" -> TopologyToDeploy.Application("service-test-source").makeSimple(),
          "processor" -> TopologyToDeploy.Application("service-test-processor").makeSimple(Some("test.processor"))
        ).toMapByApplicationId,
        aliases = LocalAliases(
          topics = Map(
            LocalAliasId("all.topics") -> Seq(FlexibleNodeId.Regex("""test.*\.topic\d""")),
            LocalAliasId("topics") -> Seq(FlexibleNodeId.Regex("""test\.topic\d"""))
          ),
          applications = Map(LocalAliasId("applications") -> Seq(FlexibleNodeId.Regex(""".*""")))
        ),
        relationships = Map(
          toDeployRelationshipFrom("source", (RelationshipType.Produce(), Seq(("topics", None)))),
          toDeployRelationshipFrom("processor", (RelationshipType.Consume(), Seq(("all.topics", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      Seq(sharedTopology, topology).toMapByTopologyId,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.shared.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Topic("test.shared.topic2", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-shared-producer", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic1", "User:service-test-shared-producer", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic1", "User:service-test-shared-producer", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic2", "User:service-test-shared-producer", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic2", "User:service-test-shared-producer", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test.shared"))) ++
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Topic("test.topic2", 1, 3, config = Map("retention.ms" -> "31536000000")).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-test-source", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-source", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-source", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-source", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-source", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test.processor", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test"))) ++
      Map(
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic1", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic1", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic2", "User:service-test-processor", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.shared.topic2", "User:service-test-processor", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple
      ).mapValues(_.withOwnerTopologyIds(Set(TopologyEntityId("test.shared"), TopologyEntityId("test"))))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "a simple connect / connect-replicator topology" should "generate the correct kafka cluster items" in {
    val topology: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test"),
        topics = Map(
          "topic1" -> TopologyToDeploy.Topic("test.topic1"),
          "topic2" -> TopologyToDeploy.Topic("test.topic2"),
          "topic1-compact" -> TopologyToDeploy.Topic("test.topic1.compact"),
          "topic2-compact" -> TopologyToDeploy.Topic("test.topic2.compact")
        ).toMapByTopicId,
        applications = Map(
          "connector" -> TopologyToDeploy.Application("service-connector").makeConnector("connector-id"),
          "connect-replicator" -> TopologyToDeploy.Application("service-connect-replicator").makeConnectReplicator("connect-replicator-id")
        ).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("connector", (RelationshipType.Produce(), Seq(("topic1", None), ("topic2", None)))),
          toDeployRelationshipFrom("connect-replicator",
            (RelationshipType.Consume(), Seq(("topic1", None), ("topic2", None))),
            (RelationshipType.Produce(), Seq(("topic1-compact", None), ("topic2-compact", None)))
          ),
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      noCurrentTopologies,
      isKafkaClusterSecurityEnabled = true,
      topologyId = TopologyEntityId("test"),
      topology,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test.topic1", 1, 3, config = Map.empty).toTuple,
        KafkaClusterItem.Topic("test.topic2", 1, 3, config = Map.empty).toTuple,
        KafkaClusterItem.Topic("test.topic1.compact", 1, 3, config = Map.empty).toTuple,
        KafkaClusterItem.Topic("test.topic2.compact", 1, 3, config = Map.empty).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-connector", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-connect-replicator", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "*", "User:service-connector", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.PREFIXED, "connect-", "User:service-connector", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "connect-", "User:service-connector", "*", AclOperation.ALL, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "*", "User:service-connect-replicator", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.PREFIXED, "connect-", "User:service-connect-replicator", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "connect-replicator-id", "User:service-connect-replicator", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "connect-", "User:service-connect-replicator", "*", AclOperation.ALL, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "_confluent", "User:service-connect-replicator", "*", AclOperation.ALL, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-connector", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-connector", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-connector", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-connector", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-connect-replicator", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1", "User:service-connect-replicator", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-connect-replicator", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2", "User:service-connect-replicator", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1.compact", "User:service-connect-replicator", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic1.compact", "User:service-connect-replicator", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2.compact", "User:service-connect-replicator", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic2.compact", "User:service-connect-replicator", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test")))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }

  "two simple connect topologies with the same user" should "generate the correct kafka cluster items" in {
    val topology1: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test1"),
        topics = Map(
          "topic" -> TopologyToDeploy.Topic("test1.topic")
        ).toMapByTopicId,
        applications = Map(
          "connector" -> TopologyToDeploy.Application("service-connector").makeConnector("connector-id")
        ).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("connector", (RelationshipType.Produce(), Seq(("topic", None))))
        )
      )

    val topology2: TopologyToDeploy =
      TopologyToDeploy(
        Namespace("test2"),
        topics = Map(
          "topic" -> TopologyToDeploy.Topic("test2.topic")
        ).toMapByTopicId,
        applications = Map(
          "connector" -> TopologyToDeploy.Application("service-connector").makeConnector("connector-id")
        ).toMapByApplicationId,
        relationships = Map(
          toDeployRelationshipFrom("connector", (RelationshipType.Produce(), Seq(("topic", None))))
        )
      )

    val actualKafkaClusterItems = KafkaClusterItems.forAllTopologies(
      kafkaCluster.resolveTopicConfig,
      Map(TopologyEntityId("test1") -> topology1, TopologyEntityId("test2") -> topology2),
      isKafkaClusterSecurityEnabled = true,
      topicDefaults
    ).map(i => (i.kafkaClusterItem.key, i)).toMap

    val expectedKafkaClusterItems =
      Map(
        KafkaClusterItem.Topic("test1.topic", 1, 3, config = Map.empty).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test1.topic", "User:service-connector", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test1.topic", "User:service-connector", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test1"))) ++
      Map(
        KafkaClusterItem.Topic("test2.topic", 1, 3, config = Map.empty).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test2.topic", "User:service-connector", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test2.topic", "User:service-connector", "*", AclOperation.WRITE, AclPermissionType.ALLOW).toTuple,
      ).mapValues(_.withOwnerTopologyId(TopologyEntityId("test2"))) ++
      // the common ACLs belong to both topologies
      Map(
        KafkaClusterItem.Acl(ResourceType.CLUSTER, PatternType.LITERAL, "kafka-cluster", "User:service-connector", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "*", "User:service-connector", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.PREFIXED, "connect-", "User:service-connector", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple,
        KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.PREFIXED, "connect-", "User:service-connector", "*", AclOperation.ALL, AclPermissionType.ALLOW).toTuple,
      ).mapValues(_.withOwnerTopologyIds(Set(TopologyEntityId("test1"), TopologyEntityId("test2"))))

    assert(actualKafkaClusterItems == expectedKafkaClusterItems)
  }
}
