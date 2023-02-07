/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import com.mwam.kafkakewl.common.ValidationResultMatchers
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology.{TopologyEntityId, TopologyId, TopologyToDeploy}
import com.mwam.kafkakewl.domain.{TestTopologiesToDeploy, TestTopologiesToDeployCommon}
import org.scalatest.{Matchers, WordSpec}

class TopologyToDeployValidatorDependentSpec extends WordSpec
  with Matchers
  with ValidationResultMatchers
  with TestTopologiesToDeploy
  with TestTopologiesToDeployCommon {

  val kafkaCluster: KafkaCluster = KafkaCluster(
    KafkaClusterEntityId("test"),
    "broker1,broker2,broker3"
  )

  val topicDefaults: TopicDefaults = TopicDefaults()

  "a new simple standalone topology" when {
    "there is another identical topology" should {
      "be invalid" in {
        val topologyToDeploy = topologyToDeploySourceSink()
        val otherTopologyToDeploy = topologyToDeploy.copy(topology = TopologyId("other"))
        val actualValidationResult = TopologyToDeployValidator.validateTopology(
          Map(TopologyEntityId("test.other") -> otherTopologyToDeploy),
          TopologyEntityId("test"),
          Some(topologyToDeploy),
          kafkaCluster.kafkaCluster,
          kafkaCluster,
          topicDefaults
        )
        actualValidationResult should beInvalid
        actualValidationResult should containMessages(
          "Topic 'test.topic' already exists in the kafka cluster",
          "Consumer group 'test.sink' already used in the kafka cluster",
          "Transactional Id 'test.sink' already used in the kafka cluster")
      }
    }
  }

  "a new kafka-streams standalone topology" when {
    "there is another identical topology" should {
      "be invalid" in {
        val topologyToDeploy = topologyToDeployKafkaStreams()
        val otherTopologyToDeploy = topologyToDeploy.copy(topology = TopologyId("other"))
        val actualValidationResult = TopologyToDeployValidator.validateTopology(
          Map(TopologyEntityId("test.other") -> otherTopologyToDeploy),
          TopologyEntityId("test"),
          Some(topologyToDeploy),
          kafkaCluster.kafkaCluster,
          kafkaCluster,
          topicDefaults
        )
        actualValidationResult should beInvalid
        actualValidationResult should containMessages(
          "Topic 'test.topic-input' already exists in the kafka cluster",
          "Topic 'test.topic-intermediate' already exists in the kafka cluster",
          "Topic 'test.topic-output' already exists in the kafka cluster",
          "Consumer group 'test.__kstreams__.Processor' already used in the kafka cluster",
          "Transactional Id 'test.__kstreams__.Processor' already used in the kafka cluster")
      }
    }
  }

  "a new connector topology" when {
    "there is another identical topology" should {
      "be invalid" in {
        val topologyToDeploy = topologyToDeployConnector()
        val otherTopologyToDeploy = topologyToDeploy.copy(topology = TopologyId("other"))
        val actualValidationResult = TopologyToDeployValidator.validateTopology(
          Map(TopologyEntityId("test.other") -> otherTopologyToDeploy),
          TopologyEntityId("test"),
          Some(topologyToDeploy),
          kafkaCluster.kafkaCluster,
          kafkaCluster,
          topicDefaults
        )
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("Topic 'test.topic-output' already exists in the kafka cluster")
      }
    }
  }

  "a new connect-replicator topology" when {
    "there is another identical topology" should {
      "be invalid" in {
        val topologyToDeploy = topologyToDeployConnectReplicator()
        val otherTopologyToDeploy = topologyToDeploy.copy(topology = TopologyId("other"))
        val actualValidationResult = TopologyToDeployValidator.validateTopology(
          Map(TopologyEntityId("test.other") -> otherTopologyToDeploy),
          TopologyEntityId("test"),
          Some(topologyToDeploy),
          kafkaCluster.kafkaCluster,
          kafkaCluster,
          topicDefaults
        )
        actualValidationResult should beInvalid
        actualValidationResult should containMessages(
          "Topic 'test.topic-input' already exists in the kafka cluster",
          "Topic 'test.topic-output' already exists in the kafka cluster")
      }
    }
  }

  "a topology with an un-managed topic" when {
    "there is another topology with the same managed topic" should {
      "be invalid" in {
        val existingTopologyToDeploy = TopologyToDeploy(topology = TopologyId("test1"), topics = Map("connect-offsets" -> TopologyToDeploy.Topic("connect-offsets")).toMapByTopicId)
        val topologyToDeploy = TopologyToDeploy(topology = TopologyId("test0"), topics = Map("connect-offsets" -> TopologyToDeploy.Topic("connect-offsets", unManaged = true)).toMapByTopicId)
        val actualValidationResult = TopologyToDeployValidator.validateTopology(
          Map(existingTopologyToDeploy.topologyEntityId -> existingTopologyToDeploy),
          topologyToDeploy.topologyEntityId,
          Some(topologyToDeploy),
          kafkaCluster.kafkaCluster,
          kafkaCluster,
          topicDefaults
        )
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("Topic 'connect-offsets' already exists in the kafka cluster")
      }
    }

    "there are other topologies with the same un-managed topic" should {
      "be invalid" in {
        val existingTopologyToDeploy1 = TopologyToDeploy(topology = TopologyId("test1"), topics = Map("connect-offsets" -> TopologyToDeploy.Topic("connect-offsets", unManaged = true)).toMapByTopicId)
        val existingTopologyToDeploy2 = TopologyToDeploy(topology = TopologyId("test2"), topics = Map("connect-offsets" -> TopologyToDeploy.Topic("connect-offsets", unManaged = true)).toMapByTopicId)
        val topologyToDeploy = TopologyToDeploy(topology = TopologyId("test0"), topics = Map("connect-offsets" -> TopologyToDeploy.Topic("connect-offsets", unManaged = true)).toMapByTopicId)
        val actualValidationResult = TopologyToDeployValidator.validateTopology(
          Map(existingTopologyToDeploy1.topologyEntityId -> existingTopologyToDeploy1, existingTopologyToDeploy2.topologyEntityId -> existingTopologyToDeploy2),
          topologyToDeploy.topologyEntityId,
          Some(topologyToDeploy),
          kafkaCluster.kafkaCluster,
          kafkaCluster,
          topicDefaults
        )
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("Topic 'connect-offsets' already exists in the kafka cluster")
      }
    }
  }

  "a topology with a managed topic" when {
    "there is another topology with the same managed topic" should {
      "be invalid" in {
        val existingTopologyToDeploy = TopologyToDeploy(topology = TopologyId("test1"), topics = Map("connect-offsets" -> TopologyToDeploy.Topic("connect-offsets")).toMapByTopicId)
        val topologyToDeploy = TopologyToDeploy(topology = TopologyId("test0"), topics = Map("connect-offsets" -> TopologyToDeploy.Topic("connect-offsets")).toMapByTopicId)
        val actualValidationResult = TopologyToDeployValidator.validateTopology(
          Map(existingTopologyToDeploy.topologyEntityId -> existingTopologyToDeploy),
          topologyToDeploy.topologyEntityId,
          Some(topologyToDeploy),
          kafkaCluster.kafkaCluster,
          kafkaCluster,
          topicDefaults
        )
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("Topic 'connect-offsets' already exists in the kafka cluster")
      }
    }

    "there is another topology with the same un-managed topic" should {
      "be invalid" in {
        val existingTopologyToDeploy1 = TopologyToDeploy(topology = TopologyId("test1"), topics = Map("connect-offsets" -> TopologyToDeploy.Topic("connect-offsets", unManaged = true)).toMapByTopicId)
        val existingTopologyToDeploy2 = TopologyToDeploy(topology = TopologyId("test2"), topics = Map("connect-offsets" -> TopologyToDeploy.Topic("connect-offsets", unManaged = true)).toMapByTopicId)
        val topologyToDeploy = TopologyToDeploy(topology = TopologyId("test0"), topics = Map("connect-offsets" -> TopologyToDeploy.Topic("connect-offsets")).toMapByTopicId)
        val actualValidationResult = TopologyToDeployValidator.validateTopology(
          Map(existingTopologyToDeploy1.topologyEntityId -> existingTopologyToDeploy1, existingTopologyToDeploy2.topologyEntityId -> existingTopologyToDeploy2),
          topologyToDeploy.topologyEntityId,
          Some(topologyToDeploy),
          kafkaCluster.kafkaCluster,
          kafkaCluster,
          topicDefaults
        )
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("Topic 'connect-offsets' already exists in the kafka cluster")
      }
    }
  }
}
