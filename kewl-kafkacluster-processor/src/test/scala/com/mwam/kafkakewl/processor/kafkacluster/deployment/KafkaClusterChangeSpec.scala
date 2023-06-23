/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.deployment

import cats.syntax.option._
import com.mwam.kafkakewl.domain.kafka.config.TopicConfigKeys.confluentPlacementConstraints
import com.mwam.kafkakewl.domain.kafkacluster.{IsTopicConfigValueEquivalent, KafkaCluster, ReplicaPlacementConfigEquivalents, TopicConfigKeyConstraint, TopicConfigKeyConstraintInclusive}
import org.scalatest.{FlatSpec, Matchers}

class KafkaClusterChangeSpec extends FlatSpec with Matchers {
  val fourConfluentPlacementConstraints: String =
    """{
      |  "version": 1,
      |  "replicas": [
      |    { "count": 2, "constraints": {"rack": "east-1"} },
      |    { "count": 2, "constraints": {"rack": "east-2"} }
      |  ],
      |  "observers": [
      |  ]
      |}
      |""".stripMargin

  val twoTwoConfluentPlacementConstraints: String =
    """{
      |  "version": 1,
      |  "replicas": [
      |    { "count": 1, "constraints": {"rack": "east-1"} },
      |    { "count": 1, "constraints": {"rack": "east-2"} }
      |  ],
      |  "observers": [
      |    { "count": 1, "constraints": {"rack": "west-1"} },
      |    { "count": 1, "constraints": {"rack": "west-2"} }
      |  ]
      |}
      |""".stripMargin

  val threeOneConfluentPlacementConstraints: String =
    """{
      |  "version": 1,
      |  "replicas": [
      |    { "count": 1, "constraints": {"rack": "east-1"} },
      |    { "count": 1, "constraints": {"rack": "east-2"} },
      |    { "count": 1, "constraints": {"rack": "west-1"} }
      |  ],
      |  "observers": [
      |    { "count": 1, "constraints": {"rack": "west-2"} }
      |  ]
      |}
      |""".stripMargin

  val topic: KafkaClusterItem.Topic = KafkaClusterItem.Topic("topic", config = Map.empty)
  val topicWithCompaction: KafkaClusterItem.Topic = KafkaClusterItem.Topic("topic", config = Map("cleanup.policy" -> "compact"))
  val topicWithRFOne: KafkaClusterItem.Topic = topic.withReplicationFactor(1)
  val topicWithTwoPartitions: KafkaClusterItem.Topic = topic.withPartitions(2)
  val topicWithFourPlacement: KafkaClusterItem.Topic = topic.withConfig(confluentPlacementConstraints, fourConfluentPlacementConstraints).withReplicationFactor(-1)
  val topicWithTwoTwoPlacement: KafkaClusterItem.Topic = topic.withConfig(confluentPlacementConstraints, twoTwoConfluentPlacementConstraints).withReplicationFactor(-1)
  val topicWithThreeOnePlacement: KafkaClusterItem.Topic = topic.withConfig(confluentPlacementConstraints, threeOneConfluentPlacementConstraints).withReplicationFactor(-1)
  val topicWithCompactionAndFourPlacement: KafkaClusterItem.Topic = topicWithFourPlacement.withConfig("cleanup.policy", "compact")
  val topicWithCompactionAndTwoTwoPlacement: KafkaClusterItem.Topic = topicWithTwoTwoPlacement.withConfig("cleanup.policy", "compact")
  val topicWithTwoPartitionsAndTwoTwoPlacement: KafkaClusterItem.Topic = topicWithTwoTwoPlacement.withPartitions(2)
  val topicWithUnmanagedConfig: KafkaClusterItem.Topic = topic.withConfig("leader.replication.throttled.replicas", "1")
  val topicWithUnmanagedConfigOtherValue: KafkaClusterItem.Topic = topic.withConfig("leader.replication.throttled.replicas", "2")
  val topicWithUnmanagedConfigAndCompaction: KafkaClusterItem.Topic = topicWithUnmanagedConfig.withConfig("cleanup.policy", "compact")
  val topicWithUnmanagedConfigOtherValueAndCompaction: KafkaClusterItem.Topic = topicWithUnmanagedConfigOtherValue.withConfig("cleanup.policy", "compact")

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

  val replicaPlacementConfigEquivalents: ReplicaPlacementConfigEquivalents = Map(
    fourConfluentPlacementConstraints -> Seq(twoTwoConfluentPlacementConstraints)
  )

  def updateTopic(before: KafkaClusterItem.Topic, after: KafkaClusterItem.Topic): KafkaClusterChange.UpdateTopic = KafkaClusterChange.UpdateTopic(before, after)

  def isManaged(topicConfigKey: String): Boolean = {
    defaultTopicConfigsForTopologies.isIncluded(topicConfigKey)
  }

  def isManaged(additionalManagedTopicConfigKeys: Set[String])(topicConfigKey: String): Boolean = {
    defaultTopicConfigsForTopologies.isIncluded(topicConfigKey) || additionalManagedTopicConfigKeys.contains(topicConfigKey)
  }

  val isTopicConfigValueEquivalent: IsTopicConfigValueEquivalent = KafkaCluster.isTopicConfigValueEquivalent(replicaPlacementConfigEquivalents, _, _, _)

  "topic-updates without any non-managed configs" should "work" in {
    updateTopic(topic, topic).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topic, topic).some
    updateTopic(topic, topicWithRFOne).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topic, topicWithRFOne).some
    updateTopic(topicWithRFOne, topic).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithRFOne, topic).some
    updateTopic(topic, topicWithCompaction).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topic, topicWithCompaction).some
    updateTopic(topicWithCompaction, topic).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithCompaction, topic).some
    updateTopic(topic, topicWithTwoPartitions).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topic, topicWithTwoPartitions).some
    updateTopic(topicWithTwoPartitions, topic).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithTwoPartitions, topic).some
  }

  "topic-updates with confluent placement constraints in the before only" should "work" in {
    updateTopic(topicWithTwoTwoPlacement, topic).ignoreNotManagedTopicConfigs(isManaged) shouldBe none
    updateTopic(topicWithTwoTwoPlacement, topicWithRFOne).ignoreNotManagedTopicConfigs(isManaged) shouldBe none
    updateTopic(topicWithTwoPartitionsAndTwoTwoPlacement, topicWithTwoPartitions).ignoreNotManagedTopicConfigs(isManaged) shouldBe none
    updateTopic(topicWithCompactionAndTwoTwoPlacement, topicWithCompaction).ignoreNotManagedTopicConfigs(isManaged) shouldBe none
    updateTopic(topicWithTwoTwoPlacement, topicWithCompaction).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithTwoTwoPlacement, topicWithCompactionAndTwoTwoPlacement).some
    updateTopic(topicWithTwoTwoPlacement, topicWithTwoPartitions).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithTwoTwoPlacement, topicWithTwoPartitionsAndTwoTwoPlacement).some
  }

  "topic-updates with confluent placement constraints in the after only" should "work" in {
    updateTopic(topic, topicWithTwoTwoPlacement).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topic, topicWithTwoTwoPlacement).some
    updateTopic(topic, topicWithTwoTwoPlacement.withReplicationFactor(3)).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topic, topicWithTwoTwoPlacement.withReplicationFactor(3)).some
    updateTopic(topicWithRFOne, topicWithTwoTwoPlacement).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithRFOne, topicWithTwoTwoPlacement).some
    updateTopic(topicWithCompaction, topicWithTwoTwoPlacement).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithCompaction, topicWithTwoTwoPlacement).some
    updateTopic(topicWithCompaction, topicWithCompactionAndTwoTwoPlacement).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithCompaction, topicWithCompactionAndTwoTwoPlacement).some
    updateTopic(topicWithTwoPartitions, topicWithTwoTwoPlacement).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithTwoPartitions, topicWithTwoTwoPlacement).some
    updateTopic(topicWithCompaction, topicWithTwoPartitionsAndTwoTwoPlacement).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithCompaction, topicWithTwoPartitionsAndTwoTwoPlacement).some
  }

  "topic-updates with confluent placement constraints in the before and after" should "work" in {
    updateTopic(topicWithTwoTwoPlacement, topicWithTwoTwoPlacement).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithTwoTwoPlacement, topicWithTwoTwoPlacement).some
    updateTopic(topicWithThreeOnePlacement, topicWithThreeOnePlacement).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithThreeOnePlacement, topicWithThreeOnePlacement).some
    updateTopic(topicWithTwoTwoPlacement, topicWithThreeOnePlacement).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithTwoTwoPlacement, topicWithThreeOnePlacement).some
    updateTopic(topicWithThreeOnePlacement, topicWithTwoTwoPlacement).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithThreeOnePlacement, topicWithTwoTwoPlacement).some
    updateTopic(topicWithTwoTwoPlacement, topicWithCompactionAndTwoTwoPlacement).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithTwoTwoPlacement, topicWithCompactionAndTwoTwoPlacement).some
    updateTopic(topicWithCompactionAndTwoTwoPlacement, topicWithTwoTwoPlacement).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithCompactionAndTwoTwoPlacement, topicWithTwoTwoPlacement).some
    updateTopic(topicWithTwoTwoPlacement, topicWithTwoPartitionsAndTwoTwoPlacement).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithTwoTwoPlacement, topicWithTwoPartitionsAndTwoTwoPlacement).some
    updateTopic(topicWithTwoPartitionsAndTwoTwoPlacement, topicWithTwoTwoPlacement).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithTwoPartitionsAndTwoTwoPlacement, topicWithTwoTwoPlacement).some
  }

  "topic-updates with unmanaged config in the before only" should "work" in {
    updateTopic(topicWithUnmanagedConfig, topic).ignoreNotManagedTopicConfigs(isManaged) shouldBe none
    updateTopic(topicWithUnmanagedConfig, topicWithCompaction).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithUnmanagedConfig, topicWithUnmanagedConfigAndCompaction).some
  }

  "topic-updates with unmanaged config in the after only" should "work" in {
    updateTopic(topic, topicWithUnmanagedConfig).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topic, topicWithUnmanagedConfig).some
    updateTopic(topicWithCompaction, topicWithUnmanagedConfig).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithCompaction, topicWithUnmanagedConfig).some
  }

  "topic-updates with unmanaged config in the before and after" should "work" in {
    updateTopic(topicWithUnmanagedConfig, topicWithUnmanagedConfigOtherValue).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithUnmanagedConfig, topicWithUnmanagedConfigOtherValue).some
    updateTopic(topicWithUnmanagedConfigAndCompaction, topicWithUnmanagedConfigOtherValueAndCompaction).ignoreNotManagedTopicConfigs(isManaged) shouldBe updateTopic(topicWithUnmanagedConfigAndCompaction, topicWithUnmanagedConfigOtherValueAndCompaction).some
  }

  "topic-updates with the same confluent-placement-constraints" should "work" in {
    updateTopic(topicWithTwoTwoPlacement, topicWithTwoTwoPlacement).ignoreEquivalentTopicConfigs(isTopicConfigValueEquivalent) shouldBe updateTopic(topicWithTwoTwoPlacement, topicWithTwoTwoPlacement).some
    updateTopic(topicWithTwoTwoPlacement, topicWithCompactionAndTwoTwoPlacement).ignoreEquivalentTopicConfigs(isTopicConfigValueEquivalent) shouldBe updateTopic(topicWithTwoTwoPlacement, topicWithCompactionAndTwoTwoPlacement).some
  }

  "topic-updates with the where the confluent-placement-constraints is set only either in the before or in the after" should "work" in {
    updateTopic(topicWithCompaction, topicWithCompactionAndTwoTwoPlacement).ignoreEquivalentTopicConfigs(isTopicConfigValueEquivalent) shouldBe updateTopic(topicWithCompaction, topicWithCompactionAndTwoTwoPlacement).some
    updateTopic(topicWithCompactionAndTwoTwoPlacement, topicWithCompaction).ignoreEquivalentTopicConfigs(isTopicConfigValueEquivalent) shouldBe updateTopic(topicWithCompactionAndTwoTwoPlacement, topicWithCompaction).some
  }

  "topic-updates with different but equivalent confluent-placement-constraints set in before and after" should "work" in {
    updateTopic(topicWithTwoTwoPlacement, topicWithFourPlacement).ignoreEquivalentTopicConfigs(isTopicConfigValueEquivalent) shouldBe none
    updateTopic(topicWithTwoTwoPlacement, topicWithCompactionAndFourPlacement).ignoreEquivalentTopicConfigs(isTopicConfigValueEquivalent) shouldBe updateTopic(topicWithTwoTwoPlacement, topicWithCompactionAndTwoTwoPlacement).some
  }

  "topic-updates with different and not equivalent confluent-placement-constraints set in before and after" should "work" in {
    updateTopic(topicWithFourPlacement, topicWithTwoTwoPlacement).ignoreEquivalentTopicConfigs(isTopicConfigValueEquivalent) shouldBe updateTopic(topicWithFourPlacement, topicWithTwoTwoPlacement).some
    updateTopic(topicWithCompactionAndFourPlacement, topicWithCompactionAndTwoTwoPlacement).ignoreEquivalentTopicConfigs(isTopicConfigValueEquivalent) shouldBe updateTopic(topicWithCompactionAndFourPlacement, topicWithCompactionAndTwoTwoPlacement).some
  }
}
