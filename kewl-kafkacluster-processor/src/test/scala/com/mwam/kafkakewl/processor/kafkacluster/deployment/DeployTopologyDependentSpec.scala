/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.deployment

import com.mwam.kafkakewl.domain.topology._
import com.mwam.kafkakewl.domain.{FlexibleName, TestTopologiesCommon, TestTopologiesToDeploy, TestTopologiesToDeployCommon}
import org.scalatest.{WordSpec, _}

class DeployTopologyDependentSpec extends WordSpec
  with Matchers
  with DeployTopologyCommon
  with TestTopologiesCommon
  with TestTopologiesToDeploy
  with TestTopologiesToDeployCommon {

  "deploying a topology consuming a shared topic in another topology" when {
    "there are no other deployed topologies" should {
      "generates ADD changes for the ACLs" in {
        val deployedSharedTopologies = Map(deployed(topologyToDeploySharedConsumableTopic(), "shared"))
        val deployedSharedTopologiesItems = kafkaClusterItemsMapOf(deployedSharedTopologies, isKafkaClusterSecurityEnabled)
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          deployedSharedTopologies,
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeployConsumingSharedTopic())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeployConsumingSharedTopic())) ++ deployedSharedTopologies, isKafkaClusterSecurityEnabled)
            .without(deployedSharedTopologiesItems)
            .mapValues(KafkaClusterChange.Add)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }
  }

  "un-deploying a topology consuming a shared topic in another topology" when {
    "there are a few other deployed topologies" should {
      "generates REMOVE changes for the ACLs" in {
        val deployedSharedTopologies = Map(deployed(topologyToDeploySharedConsumableTopic(), "shared"))
        val deployedSharedTopologiesItems = kafkaClusterItemsMapOf(deployedSharedTopologies, isKafkaClusterSecurityEnabled)
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          deployedSharedTopologies ++
            Map(
              deployed(topologyToDeployConsumingSharedTopic()),
              deployed(topologyToDeploySourceSink("test2"), "test2"),
              deployed(topologyToDeploySourceSink("test3"), "test3"),
              deployed(topologyToDeploySourceSink("test4"), "test4")
            ),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          None
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeployConsumingSharedTopic())) ++ deployedSharedTopologies, isKafkaClusterSecurityEnabled)
            .without(deployedSharedTopologiesItems)
            .mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }
  }

  "deploying an application user change in a topology consuming a shared topic in another topology" when {
    "there are no other deployed topologies" should {
      "generates REMOVE and ADD changes for the ACLs" in {
        val deployedSharedTopologies = Map(deployed(topologyToDeploySharedConsumableTopic(), "shared"))
        val deployedSharedTopologiesItems = kafkaClusterItemsMapOf(deployedSharedTopologies, isKafkaClusterSecurityEnabled)
        // changing the user-name of the application
        val updatedTopology = topologyToDeployConsumingSharedTopic() ~+ (LocalApplicationId("sink") --> TopologyToDeploy.Application(s"service-test-other").makeSimple(Some(s"test.sink")))
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          deployedSharedTopologies ++ Map(deployed(topologyToDeployConsumingSharedTopic(), "test")),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(updatedTopology)
        )

        val expectedKafkaClusterRemoveChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeployConsumingSharedTopic())) ++ deployedSharedTopologies, isKafkaClusterSecurityEnabled)
            .without(deployedSharedTopologiesItems)
            .mapValues(KafkaClusterChange.Remove)
        val expectedKafkaClusterAddChanges =
          kafkaClusterItemsMapOf(Map(deployed(updatedTopology)) ++ deployedSharedTopologies, isKafkaClusterSecurityEnabled)
            .without(deployedSharedTopologiesItems)
            .mapValues(KafkaClusterChange.Add)

        actualKafkaClusterChanges shouldBe (expectedKafkaClusterRemoveChanges ++ expectedKafkaClusterAddChanges)
      }
    }
  }

  "deploying a topology producing a shared topic in another topology" when {
    "there are no other deployed topologies" should {
      "generates ADD changes for the ACLs" in {
        val deployedSharedTopologies = Map(deployed(topologyToDeploySharedProducableTopic(), "shared"))
        val deployedSharedTopologiesItems = kafkaClusterItemsMapOf(deployedSharedTopologies, isKafkaClusterSecurityEnabled)
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          deployedSharedTopologies,
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeployProducingSharedTopic())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeployProducingSharedTopic())) ++ deployedSharedTopologies, isKafkaClusterSecurityEnabled)
            .without(deployedSharedTopologiesItems)
            .mapValues(KafkaClusterChange.Add)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }
  }

  "deploying a topology with a topic consumed by a shared application in another topology" when {
    "there are no other deployed topologies" should {
      "generates ADD changes for the ACLs" in {
        val deployedSharedTopologies = Map(deployed(topologyToDeploySharedConsumingApplication(), "shared"))
        val deployedSharedTopologiesItems = kafkaClusterItemsMapOf(deployedSharedTopologies, isKafkaClusterSecurityEnabled)
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          deployedSharedTopologies,
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeployWithTopicConsumedBySharedConsumingApplication())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeployWithTopicConsumedBySharedConsumingApplication())) ++ deployedSharedTopologies, isKafkaClusterSecurityEnabled)
            .without(deployedSharedTopologiesItems)
            .mapValues(KafkaClusterChange.Add)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }
  }

  "deploying a topology with a topic produced by a shared application in another topology" when {
    "there are no other deployed topologies" should {
      "generates ADD changes for the ACLs" in {
        val deployedSharedTopologies = Map(deployed(topologyToDeploySharedProducingApplication(), "shared"))
        val deployedSharedTopologiesItems = kafkaClusterItemsMapOf(deployedSharedTopologies, isKafkaClusterSecurityEnabled)
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          deployedSharedTopologies,
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeployWithTopicProducedBySharedProducingApplication())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeployWithTopicProducedBySharedProducingApplication())) ++ deployedSharedTopologies, isKafkaClusterSecurityEnabled)
            .without(deployedSharedTopologiesItems)
            .mapValues(KafkaClusterChange.Add)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }
  }

  "deploying a shared topology with a new topic" when {
    "there is another deployed topology with a regex alias relationship matching the topic" should {
      "generates ADD changes for the ACLs and the topic for both topologies" in {
        val sharedDeployedTopology = TopologyToDeploy(
          Namespace("shared"),
          topics = Map(
            "topic-1" -> TopologyToDeploy.Topic(s"shared.topic-1", otherConsumerNamespaces = Seq(FlexibleName.Any())),
            "topic-2" -> TopologyToDeploy.Topic(s"shared.topic-2", otherConsumerNamespaces = Seq(FlexibleName.Any()))
          ).toMapByTopicId
        )

        val deployedTopology = TopologyToDeploy(
          Namespace("test"),
          applications = Map(
            "sink-1" -> TopologyToDeploy.Application("service-test").makeSimple(Some("test.sink-1")),
          ).toMapByApplicationId,
          aliases = LocalAliases(
            topics = Map("shared-topics" -> FlexibleNodeId.Namespace("shared")).toAliases
          ),
          relationships = Map(
            toDeployRelationshipFrom(s"sink-1", (RelationshipType.Consume(), Seq((s"shared-topics", None))))
          )
        )

        val sharedDeployedTopologyUpdated = TopologyToDeploy(
          Namespace("shared"),
          topics = Map(
            "topic-1" -> TopologyToDeploy.Topic(s"shared.topic-1", otherConsumerNamespaces = Seq(FlexibleName.Any())),
            "topic-2" -> TopologyToDeploy.Topic(s"shared.topic-2", otherConsumerNamespaces = Seq(FlexibleName.Any())),
            "topic-3" -> TopologyToDeploy.Topic(s"shared.topic-3", otherConsumerNamespaces = Seq(FlexibleName.Any()))
          ).toMapByTopicId
        )

        val deployedTopologies = Map(deployed(sharedDeployedTopology, "shared"), deployed(deployedTopology))
        val deployedTopologiesItems = kafkaClusterItemsMapOf(deployedTopologies, isKafkaClusterSecurityEnabled)
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          deployedTopologies,
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("shared"),
          Some(sharedDeployedTopologyUpdated)
        )

        val expectedKafkaClusterChanges =
          // expecting ADD changes for the new stuff minus the current stuff (the topic and 2 ACLs)
          kafkaClusterItemsMapOf(Map(deployed(sharedDeployedTopologyUpdated, "shared"), deployed(deployedTopology)), isKafkaClusterSecurityEnabled)
            .without(deployedTopologiesItems)
            .mapValues(KafkaClusterChange.Add)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }
  }

  "deploying a shared topology with one less topic" when {
    "there is another deployed topology with a regex alias relationship matching the topic" should {
      "generates REMOVE changes for the ACLs and the topic for both topologies" in {
        val sharedDeployedTopology = TopologyToDeploy(
          Namespace("shared"),
          topics = Map(
            "topic-1" -> TopologyToDeploy.Topic(s"shared.topic-1", otherConsumerNamespaces = Seq(FlexibleName.Any())),
            "topic-2" -> TopologyToDeploy.Topic(s"shared.topic-2", otherConsumerNamespaces = Seq(FlexibleName.Any()))
          ).toMapByTopicId
        )

        val deployedTopology = TopologyToDeploy(
          Namespace("test"),
          applications = Map(
            "sink-1" -> TopologyToDeploy.Application("service-test").makeSimple(Some("test.sink-1")),
          ).toMapByApplicationId,
          aliases = LocalAliases(
            topics = Map("shared-topics" -> FlexibleNodeId.Namespace("shared")).toAliases
          ),
          relationships = Map(
            toDeployRelationshipFrom(s"sink-1", (RelationshipType.Consume(), Seq((s"shared-topics", None))))
          )
        )

        val sharedDeployedTopologyUpdated = TopologyToDeploy(
          Namespace("shared"),
          topics = Map(
            "topic-1" -> TopologyToDeploy.Topic(s"shared.topic-1", otherConsumerNamespaces = Seq(FlexibleName.Any()))
          ).toMapByTopicId
        )

        val deployedTopologies = Map(deployed(sharedDeployedTopology, "shared"), deployed(deployedTopology))
        val deployedTopologiesUpdated = Map(deployed(sharedDeployedTopologyUpdated, "shared"), deployed(deployedTopology))
        val deployedTopologiesUpdatedItems = kafkaClusterItemsMapOf(deployedTopologiesUpdated, isKafkaClusterSecurityEnabled)
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          deployedTopologies,
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("shared"),
          Some(sharedDeployedTopologyUpdated)
        )

        val expectedKafkaClusterChanges =
          // expecting REMOVE changes for the current stuff minus the new stuff (the topic and 2 ACLs)
          kafkaClusterItemsMapOf(Map(deployed(sharedDeployedTopology, "shared"), deployed(deployedTopology)), isKafkaClusterSecurityEnabled)
            .without(deployedTopologiesUpdatedItems)
            .mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }
  }
}
