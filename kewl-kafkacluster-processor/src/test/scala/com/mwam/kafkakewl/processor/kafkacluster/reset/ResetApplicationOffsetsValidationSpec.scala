/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.reset

import com.mwam.kafkakewl.common.ValidationResultMatchers
import com.mwam.kafkakewl.common.topology.TopologyToDeployOperations
import com.mwam.kafkakewl.domain.deploy.DeployedTopology.ResetApplicationOptions
import com.mwam.kafkakewl.domain.deploy.{TopicPartitionPosition, TopicPartitionPositionOfPartition}
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology.{FlexibleTopologyTopicId, LocalTopicId}
import com.mwam.kafkakewl.domain.{TestTopologiesToDeploy, TestTopologiesToDeployCommon}
import org.scalatest.{FlatSpec, Matchers}

class ResetApplicationOffsetsValidationSpec extends FlatSpec
  with Matchers
  with ValidationResultMatchers
  with ResetApplicationOffsetsCommon
  with TopologyToDeployOperations
  with ResetCommon
  with TestTopologiesToDeploy
  with TestTopologiesToDeployCommon
{
  val topicDefaults: TopicDefaults = TopicDefaults()

  "default ResetApplicationOptions.ApplicationTopics" should "be valid" in {
    val topologyToDeploy = topologyToDeploySourceSink()
    val options = ResetApplicationOptions.ApplicationTopics()
    val actual = validateTopicPartitionsInTopology(topologyToDeploy, options.expectExistingTopicPartitions(topologyToDeploy.topologyNamespace))
    actual should beValid
  }

  "ResetApplicationOptions.ApplicationTopics with no topics" should "be valid" in {
    val topologyToDeploy = topologyToDeploySourceSink()
    val options = ResetApplicationOptions.ApplicationTopics(topics = Seq.empty)
    val actual = validateTopicPartitionsInTopology(topologyToDeploy, options.expectExistingTopicPartitions(topologyToDeploy.topologyNamespace))
    actual should beValid
  }

  "ResetApplicationOptions.ApplicationTopics with some topics" should "be valid" in {
    val topologyToDeploy = topologyToDeploySourceSink()
    val options = ResetApplicationOptions.ApplicationTopics(topics = Seq(FlexibleTopologyTopicId.Exact("some-topic"), FlexibleTopologyTopicId.Prefix("test.")))
    val actual = validateTopicPartitionsInTopology(topologyToDeploy, options.expectExistingTopicPartitions(topologyToDeploy.topologyNamespace))
    actual should beValid
  }

  "ResetApplicationOptions.ApplicationTopics with position = TopicPartitionPosition.Beginning()" should "be valid" in {
    val topologyToDeploy = topologyToDeploySourceSink()
    val options = ResetApplicationOptions.ApplicationTopics(position = TopicPartitionPosition.Beginning())
    val actual = validateTopicPartitionsInTopology(topologyToDeploy, options.expectExistingTopicPartitions(topologyToDeploy.topologyNamespace))
    actual should beValid
  }

  "default ResetApplicationOptions.ApplicationTopicPositions" should "be valid" in {
    val topologyToDeploy = topologyToDeploySourceSink()
    val options = ResetApplicationOptions.ApplicationTopicPositions(Map.empty)
    val actual = validateTopicPartitionsInTopology(topologyToDeploy, options.expectExistingTopicPartitions(topologyToDeploy.topologyNamespace))
    actual should beValid
  }

  "ResetApplicationOptions.ApplicationTopicPositions with a topic that exists in the topology" should "be valid" in {
    val topologyToDeploy = topologyToDeploySourceSink()
    val options = ResetApplicationOptions.ApplicationTopicPositions(positions = Map(LocalTopicId("topic") -> TopicPartitionPosition.Beginning()))
    val actual = validateTopicPartitionsInTopology(topologyToDeploy, options.expectExistingTopicPartitions(topologyToDeploy.topologyNamespace))
    actual should beValid
  }

  "ResetApplicationOptions.ApplicationTopicPositions with a topic that does not exist in the topology" should "be valid" in {
    val topologyToDeploy = topologyToDeploySourceSink()
    val options = ResetApplicationOptions.ApplicationTopicPositions(positions = Map(LocalTopicId("other-topic") -> TopicPartitionPosition.Beginning()))
    val actual = validateTopicPartitionsInTopology(topologyToDeploy, options.expectExistingTopicPartitions(topologyToDeploy.topologyNamespace))
    actual should beInvalid
    actual should containMessage("topic 'test.other-topic' not in the topology")
  }

  "ResetApplicationOptions.ApplicationTopicPartitions with a topic/partition that exists in the topology" should "be valid" in {
    val topologyToDeploy = topologyToDeploySourceSink()
    val options = ResetApplicationOptions.ApplicationTopicPartitions(
      positions = Map(
        LocalTopicId("topic") -> Seq(TopicPartitionPositionOfPartition(0, TopicPartitionPosition.Beginning())))
    )
    val actual = validateTopicPartitionsInTopology(topologyToDeploy, options.expectExistingTopicPartitions(topologyToDeploy.topologyNamespace))
    actual should beValid
  }

  "ResetApplicationOptions.ApplicationTopicPartitions with a topic that does not exist in the topology" should "be valid" in {
    val topologyToDeploy = topologyToDeploySourceSink()
    val options = ResetApplicationOptions.ApplicationTopicPartitions(
      positions = Map(
        LocalTopicId("other-topic") -> Seq(TopicPartitionPositionOfPartition(0, TopicPartitionPosition.Beginning())))
    )
    val actual = validateTopicPartitionsInTopology(topologyToDeploy, options.expectExistingTopicPartitions(topologyToDeploy.topologyNamespace))
    actual should beInvalid
    actual should containMessage("topic 'test.other-topic' not in the topology")
  }

  "ResetApplicationOptions.ApplicationTopicPartitions with a partition that does not exist in the topology" should "be valid" in {
    val topologyToDeploy = topologyToDeploySourceSink()
    val options = ResetApplicationOptions.ApplicationTopicPartitions(
      positions = Map(
        LocalTopicId("topic") -> Seq(TopicPartitionPositionOfPartition(1, TopicPartitionPosition.Beginning())))
    )
    val actual = validateTopicPartitionsInTopology(topologyToDeploy, options.expectExistingTopicPartitions(topologyToDeploy.topologyNamespace))
    actual should beInvalid
    actual should containMessage("Topic 'test.topic' partition = 1 is invalid (number of partitions = 1)")
  }

  "ResetApplicationOptions.ApplicationTopicPartitions with a partition that's a negative number" should "be valid" in {
    val topologyToDeploy = topologyToDeploySourceSink()
    val options = ResetApplicationOptions.ApplicationTopicPartitions(
      positions = Map(
        LocalTopicId("topic") -> Seq(TopicPartitionPositionOfPartition(-1, TopicPartitionPosition.Beginning())))
    )
    val actual = validateTopicPartitionsInTopology(topologyToDeploy, options.expectExistingTopicPartitions(topologyToDeploy.topologyNamespace))
    actual should beInvalid
    actual should containMessage("Topic 'test.topic' partition = -1 is invalid")
  }
}
