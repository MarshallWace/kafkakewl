/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.deployment

import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.domain.kafkacluster.nullResolveReplicaPlacement
import com.mwam.kafkakewl.domain.topology._
import org.apache.kafka.common.acl.{AclOperation, AclPermissionType}
import org.apache.kafka.common.resource.{PatternType, ResourceType}
import org.scalatest._
import org.scalatest.WordSpec

class DeployTopologyStandaloneSpec extends WordSpec
  with Matchers
  with DeployTopologyCommon
  with TestTopologiesToDeploy
  with TestTopologiesToDeployCommon {

  "deploying an empty topology" when {

    "there are no deployed topologies" should {
      "generates no changes" in {
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          noCurrentDeployedTopologies,
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(TopologyToDeploy())
        )

        actualKafkaClusterChanges shouldBe empty
      }
    }

    "the empty topology is already deployed" should {
      "generates no changes" in {
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          Map(deployed(TopologyToDeploy(), "test")),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(TopologyToDeploy())
        )

        actualKafkaClusterChanges shouldBe empty
      }
    }

    "there are no deployed topologies but there are redundant topics and ACLs" should {
      "generates no changes" in {
        val actualKafkaClusterChanges = createDeployChanges(
          noCurrentDeployedTopologies,
          Seq(
            KafkaClusterItem.Topic("redundant.topic", 1, 3, Map.empty),
            KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "redundant.topic", "User:service-nobody", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)
          ).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(TopologyToDeploy())
        )

        actualKafkaClusterChanges shouldBe empty
      }
    }

    "there is another deployed topology" should {
      "generates no changes" in {
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          Map(deployed(topologyToDeploySourceSink("existing"), "existing")),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(TopologyToDeploy())
        )

        actualKafkaClusterChanges shouldBe empty
      }
    }

    "there is another deployed topology and redundant topics and ACLs" should {
      "generates no changes" in {
        val actualKafkaClusterChanges = createDeployChanges(
          Map(deployed(topologyToDeploySourceSink("existing"), "existing")),
          Seq(
            KafkaClusterItem.Topic("redundant.topic", 1, 3, Map.empty),
            KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "redundant.topic", "User:service-nobody", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)
          ).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(TopologyToDeploy())
        )

        actualKafkaClusterChanges shouldBe empty
      }
    }
  }

  "deploying a source-sink topology" when {
    "there are no deployed topologies" should {
      "generates ADD changes only" in {
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          noCurrentDeployedTopologies,
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .mapValues(KafkaClusterChange.Add)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no deployed topologies but there are unrelated redundant topics and ACLs" should {
      "generates ADD changes only" in {
        val actualKafkaClusterChanges = createDeployChanges(
          noCurrentDeployedTopologies,
          Seq(
            KafkaClusterItem.Topic("redundant.topic", 1, 3, Map.empty),
            KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "redundant.topic", "User:service-nobody", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)
          ).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .mapValues(KafkaClusterChange.Add)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no deployed topologies but some of the topics/ACLs already exists in the cluster" should {
      "generates ADD changes for the missing ACLs only" in {
        val alreadyExistingTopic: KafkaClusterItem = KafkaClusterItem.Topic("test.topic", 1, 3, Map("retention.ms" -> "31536000000"))
        val alreadyExistingAcl: KafkaClusterItem = KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic", "User:service-test", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)
        val alreadyExistingItems = Seq(alreadyExistingTopic, alreadyExistingAcl).map(_.toTuple).toMap
        val actualKafkaClusterChanges = createDeployChanges(
          noCurrentDeployedTopologies,
          alreadyExistingItems.values.map(_.withoutOwnerTopologyIds()).toSeq,
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            // we expect the already existing ones NOT to be in the changes
            .filterKeys(!alreadyExistingItems.contains(_))
            .mapValues(KafkaClusterChange.Add)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no deployed topologies but a topic already exists in the cluster with different config" should {
      "generates UPDATE-TOPIC change and ADD changes for the missing ACLs only" in {
        val alreadyExistingButDifferentTopic = KafkaClusterItem.Topic("test.topic", 1, 3, Map("retention.ms" -> "11536000000"))
        val alreadyExistingAcl = KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.topic", "User:service-test", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)
        val alreadyExistingItems = Seq(alreadyExistingButDifferentTopic, alreadyExistingAcl).map(_.toTuple).toMap
        val actualKafkaClusterChanges = createDeployChanges(
          noCurrentDeployedTopologies,
          alreadyExistingItems.values.map(_.withoutOwnerTopologyIds()).toSeq,
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .filterKeys(_ != alreadyExistingAcl.key)
            .mapValues {
              // for the existing topic, we expect an update;
              case after: KafkaClusterItem.Topic if after.key == alreadyExistingButDifferentTopic.key =>
                KafkaClusterChange.UpdateTopic(alreadyExistingButDifferentTopic, after)
              // for everything else but the existing ACL we expect an ADD
              case after: KafkaClusterItem if after.key != alreadyExistingAcl.key  =>
                KafkaClusterChange.Add(after)
            }

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no deployed topologies but a topic already exists in the cluster with more config keys" should {
      "generates UPDATE-TOPIC change and ADD changes for the missing ACLs only" in {
        val alreadyExistingButDifferentTopic = KafkaClusterItem.Topic("test.topic", 1, 3, Map("cleanup.policy" -> "delete"))
        val actualKafkaClusterChanges = createDeployChanges(
          noCurrentDeployedTopologies,
          Seq(alreadyExistingButDifferentTopic).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .mapValues {
              // for the existing topic, we expect an update
              case after: KafkaClusterItem.Topic if after.key == alreadyExistingButDifferentTopic.key =>
                KafkaClusterChange.UpdateTopic(alreadyExistingButDifferentTopic, after)
              // for everything else we expect an ADD
              case after: KafkaClusterItem  =>
                KafkaClusterChange.Add(after)
            }

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "topology is already deployed with some notRemoved keys which still do exists in the kafka-cluster" should {
      "generates REMOVE change for the notRemoved keys" in {
        val redundantTopic = KafkaClusterItem.Topic("existing.topic-old", 1, 3, config = Map.empty)
        val actualKafkaClusterChanges = createDeployChangesWithRedundantKafkaClusterItems(
          Map(deployed(topologyToDeploySourceSink(), notRemovedKeys = Seq(redundantTopic.key))),
          Seq(redundantTopic.withOwnerTopologyId(TopologyEntityId("test"))),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink())
        )

        val expectedKafkaClusterChanges = Map(redundantTopic.toTuple).mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "topology is already deployed with some notRemoved keys which no longer exists in the kafka-cluster" should {
      "generates no changes" in {
        val redundantTopic = KafkaClusterItem.Topic("existing.topic-old", 1, 3, config = Map.empty)
        val actualKafkaClusterChanges = createDeployChangesWithRedundantKafkaClusterItems(
          Map(deployed(topologyToDeploySourceSink(), notRemovedKeys = Seq(redundantTopic.key))),
          Seq.empty,
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink())
        )

        val expectedKafkaClusterChanges = Map.empty

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there is another deployed topology" should {
      "generates ADD changes only" in {
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          Map(deployed(topologyToDeploySourceSink("existing"), "existing")),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .mapValues(KafkaClusterChange.Add)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there is another deployed topology and redundant topics and ACLs" should {
      "generates ADD changes only" in {
        val actualKafkaClusterChanges = createDeployChanges(
          Map(deployed(topologyToDeploySourceSink("existing"), "existing")),
          Seq(
            KafkaClusterItem.Topic("redundant.topic", 1, 3, Map.empty),
            KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "redundant.topic", "User:service-nobody", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)
          ).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .mapValues(KafkaClusterChange.Add)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }
  }

  "deploying a source-sink topology with an un-managed topic" when {
    "there are no deployed topologies" should {
      "generates ADD changes only for the managed topics" in {
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          noCurrentDeployedTopologies,
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink().updateTopic("topic", topic => topic.copy(unManaged = true)))
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .collect {
              case (k, i: KafkaClusterItem.Topic) if i.name != "test.topic" => (k, i)
              case (k, i: KafkaClusterItem.Acl) => (k, i)
            }
            .mapValues(KafkaClusterChange.Add)
        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no deployed topologies but the cluster already contains the un-managed topic" should {
      "generates ADD changes only for the managed topics" in {
        val alreadyExistingTopic = KafkaClusterItem.Topic("test.topic", 1, 3, Map("retention.ms" -> "31536000000"))
        val actualKafkaClusterChanges = createDeployChanges(
          noCurrentDeployedTopologies,
          Seq(alreadyExistingTopic).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink().updateTopic("topic", topic => topic.copy(unManaged = true)))
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .collect {
              case (k, i: KafkaClusterItem.Topic) if i.name != "test.topic" => (k, i)
              case (k, i: KafkaClusterItem.Acl) => (k, i)
            }
            .mapValues(KafkaClusterChange.Add)
        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }
  }

  "re-deploying a deployed source-sink topology" when {
    "there are no other deployed topologies" should {
      "generates no changes" in {
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          Map(deployed(topologyToDeploySourceSink())),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink())
        )

        actualKafkaClusterChanges shouldBe empty
      }
    }

    "there are no other deployed topologies, but nothing in the cluster" should {
      "generates ADD changes only" in {
        val actualKafkaClusterChanges = createDeployChanges(
          Map(deployed(topologyToDeploySourceSink())),
          Seq.empty,
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .mapValues(KafkaClusterChange.Add)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no other deployed topologies, but the topic in the cluster is different" should {
      "generates ADD changes for the ACLs and UPDATE for the topic" in {
        val alreadyExistingButDifferentTopic = KafkaClusterItem.Topic("test.topic", 1, 3, Map("retention.ms" -> "61536000000"))
        val actualKafkaClusterChanges = createDeployChanges(
          Map(deployed(topologyToDeploySourceSink())),
          Seq(alreadyExistingButDifferentTopic).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .mapValues {
              // for the existing topic, we expect an update;
              case after: KafkaClusterItem.Topic if after.key == alreadyExistingButDifferentTopic.key =>
                KafkaClusterChange.UpdateTopic(alreadyExistingButDifferentTopic, after)
              // for everything else but the existing ACL we expect an ADD
              case after: KafkaClusterItem =>
                KafkaClusterChange.Add(after)
            }

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no other deployed topologies, but this topology's current deployment contains a topic and ACL in notRemovedKeys that still exist" should {
      "generates REMOVE for that topic and ACL" in {
        val notRemovedTopic = KafkaClusterItem.Topic("test.old-topic", 1, 3, Map.empty)
        val notRemovedAcl = KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.old-topic", "User:service-test", "*", AclOperation.READ, AclPermissionType.ALLOW)
        val notRemovedItems = Seq(notRemovedTopic, notRemovedAcl)
        val actualKafkaClusterChanges = createDeployChangesWithRedundantKafkaClusterItems(
          Map(deployed(topologyToDeploySourceSink(), notRemovedKeys = notRemovedItems.map(_.key))),
          notRemovedItems.map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink())
        )

        val expectedKafkaClusterChanges = notRemovedItems.map(_.toTuple).toMap.mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no other deployed topologies, but this topology's current deployment contains a topic and ACL in notRemovedKeys which does not exist" should {
      "generates no changes" in {
        val notRemovedTopic = KafkaClusterItem.Topic("test.old-topic", 1, 3, Map.empty)
        val notRemovedAcl = KafkaClusterItem.Acl(ResourceType.TOPIC, PatternType.LITERAL, "test.old-topic", "User:service-test", "*", AclOperation.READ, AclPermissionType.ALLOW)
        val notRemovedItems = Seq(notRemovedTopic, notRemovedAcl)
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          Map(deployed(topologyToDeploySourceSink(), notRemovedKeys = notRemovedItems.map(_.key))),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink())
        )

        actualKafkaClusterChanges shouldBe empty
      }
    }
  }

  "re-deploying a deployed source-sink topology with an un-managed topic" when {
    "there are no deployed topologies" should {
      "generates no changes" in {
        val topology = topologyToDeploySourceSink().updateTopic("topic", topic => topic.copy(unManaged = true))
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          Map(deployed(topology)),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topology)
        )

        actualKafkaClusterChanges shouldBe Map.empty
      }
    }

    "there are no other deployed topologies, but nothing in the cluster" should {
      "generates ADD changes only for the managed topics" in {
        val topology = topologyToDeploySourceSink().updateTopic("topic", topic => topic.copy(unManaged = true))
        val actualKafkaClusterChanges = createDeployChanges(
          Map(deployed(topology)),
          Seq.empty,
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topology)
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .collect {
              case (k, i: KafkaClusterItem.Topic) if i.name != "test.topic" => (k, i)
              case (k, i: KafkaClusterItem.Acl) => (k, i)
            }
            .mapValues(KafkaClusterChange.Add)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no other deployed topologies, but the cluster already contains the un-managed topic" should {
      "generates ADD changes for the ACLs only" in {
        val topology = topologyToDeploySourceSink().updateTopic("topic", topic => topic.copy(unManaged = true))
        val alreadyExistingButDifferentTopic = KafkaClusterItem.Topic("test.topic", 1, 3, Map("retention.ms" -> "31536000000"))
        val actualKafkaClusterChanges = createDeployChanges(
          Map(deployed(topology)),
          Seq(alreadyExistingButDifferentTopic).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topology)
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .collect { case (k, after: KafkaClusterItem.Acl) => (k, KafkaClusterChange.Add(after)) }

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no other deployed topologies, but the un-managed topic in the cluster is different" should {
      "generates ADD changes for the ACLs only" in {
        val topology = topologyToDeploySourceSink().updateTopic("topic", topic => topic.copy(unManaged = true))
        val alreadyExistingButDifferentTopic = KafkaClusterItem.Topic("test.topic", 1, 3, Map("retention.ms" -> "61536000000"))
        val actualKafkaClusterChanges = createDeployChanges(
          Map(deployed(topology)),
          Seq(alreadyExistingButDifferentTopic).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topology)
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .collect { case (k, after: KafkaClusterItem.Acl) => (k, KafkaClusterChange.Add(after)) }

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }
  }

  "deploying a new topic change" when {
    "there are no other deployed topologies" should {
      "generates an ADD change only for the new topic" in {
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          Map(deployed(topologyToDeploySourceSink())),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink() + (LocalTopicId("another-topic") --> TopologyToDeploy.Topic("test.another-topic")))
        )

        val expectedKafkaClusterChanges =
          Map(KafkaClusterItem.Topic("test.another-topic", 1, 3, Map.empty).toTuple)
            .mapValues(KafkaClusterChange.Add)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no other deployed topologies, but the new topic already exists with same the config" should {
      "generates no changes" in {
        val actualKafkaClusterChanges = createDeployChangesWithRedundantKafkaClusterItems(
          Map(deployed(topologyToDeploySourceSink())),
          Seq(
            KafkaClusterItem.Topic("test.another-topic", 1, 3, Map.empty)
          ).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink() + (LocalTopicId("another-topic") --> TopologyToDeploy.Topic("test.another-topic")))
        )

        actualKafkaClusterChanges shouldBe empty
      }
    }

    "there are no other deployed topologies, but the new topic already exists with different config" should {
      "generates an UPDATE change for the new topic" in {
        val alreadyExistingButDifferentTopic = KafkaClusterItem.Topic("test.another-topic", 1, 3, Map("retention.ms" -> "31536000000"))
        val actualKafkaClusterChanges = createDeployChangesWithRedundantKafkaClusterItems(
          Map(deployed(topologyToDeploySourceSink())),
          Seq(alreadyExistingButDifferentTopic).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink() + (LocalTopicId("another-topic") --> TopologyToDeploy.Topic("test.another-topic")))
        )

        val expectedKafkaClusterChanges =
          Map(KafkaClusterChange.UpdateTopic(alreadyExistingButDifferentTopic, KafkaClusterItem.Topic("test.another-topic", 1, 3, Map.empty)).toTuple)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no other deployed topologies, but the new topic is in the previous deployment's notRemovedKeys and it still exists with the required config" should {
      "generates no changes" in {
        val stillExistingTopic = KafkaClusterItem.Topic("test.another-topic", 1, 3, Map.empty)
        val actualKafkaClusterChanges = createDeployChangesWithRedundantKafkaClusterItems(
          Map(deployed(topologyToDeploySourceSink(), notRemovedKeys = Seq(stillExistingTopic).map(_.key))),
          Seq(stillExistingTopic).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink() + (LocalTopicId("another-topic") --> TopologyToDeploy.Topic("test.another-topic")))
        )

        actualKafkaClusterChanges shouldBe empty
      }
    }

    "there are no other deployed topologies, but the new topic is in the previous deployment's notRemovedKeys and it exists but with different config" should {
      "generates an UPDATE change for the topic" in {
        val stillExistingTopicWithDifferentConfig = KafkaClusterItem.Topic("test.another-topic", 1, 3, Map("retention.ms" -> "31536000000"))
        val actualKafkaClusterChanges = createDeployChangesWithRedundantKafkaClusterItems(
          Map(deployed(topologyToDeploySourceSink(), notRemovedKeys = Seq(stillExistingTopicWithDifferentConfig).map(_.key))),
          Seq(stillExistingTopicWithDifferentConfig).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink() + (LocalTopicId("another-topic") --> TopologyToDeploy.Topic("test.another-topic")))
        )

        val expectedKafkaClusterChanges =
          Map(KafkaClusterChange.UpdateTopic(stillExistingTopicWithDifferentConfig, KafkaClusterItem.Topic("test.another-topic", 1, 3, Map.empty)).toTuple)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no other deployed topologies, but the new topic is in the previous deployment's notRemovedKeys and it does not exist" should {
      "generates an ADD change for the topic" in {
        val nonExistingTopic = KafkaClusterItem.Topic("test.another-topic", 1, 3, Map.empty)
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          Map(deployed(topologyToDeploySourceSink(), notRemovedKeys = Seq(nonExistingTopic).map(_.key))),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink() + (LocalTopicId("another-topic") --> TopologyToDeploy.Topic("test.another-topic")))
        )

        val expectedKafkaClusterChanges =
          Map(KafkaClusterItem.Topic("test.another-topic", 1, 3, Map.empty).toTuple)
            .mapValues(KafkaClusterChange.Add)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }
  }

  "deploying a new un-managed topic change" when {
    "there are no other deployed topologies" should {
      "generates no changes" in {
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          Map(deployed(topologyToDeploySourceSink())),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink() + (LocalTopicId("another-topic") --> TopologyToDeploy.Topic("test.another-topic", unManaged = true)))
        )

        actualKafkaClusterChanges shouldBe Map.empty
      }
    }

    "there are no other deployed topologies, but the new topic already exists with same the config" should {
      "generates no changes" in {
        val actualKafkaClusterChanges = createDeployChangesWithRedundantKafkaClusterItems(
          Map(deployed(topologyToDeploySourceSink())),
          Seq(
            KafkaClusterItem.Topic("test.another-topic", 1, 3, Map.empty)
          ).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink() + (LocalTopicId("another-topic") --> TopologyToDeploy.Topic("test.another-topic", unManaged = true)))
        )

        actualKafkaClusterChanges shouldBe empty
      }
    }

    "there are no other deployed topologies, but the new topic already exists with different config" should {
      "generates no changes" in {
        val alreadyExistingButDifferentTopic = KafkaClusterItem.Topic("test.another-topic", 1, 3, Map("retention.ms" -> "31536000000"))
        val actualKafkaClusterChanges = createDeployChangesWithRedundantKafkaClusterItems(
          Map(deployed(topologyToDeploySourceSink())),
          Seq(alreadyExistingButDifferentTopic).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink() + (LocalTopicId("another-topic") --> TopologyToDeploy.Topic("test.another-topic", unManaged = true)))
        )

        actualKafkaClusterChanges shouldBe Map.empty
      }
    }

    "there are no other deployed topologies, but the new topic already exists with same the config and it's in the notRemovedKeys" should {
      "generates no changes" in {
        val redundantTopic = KafkaClusterItem.Topic("test.another-topic", 1, 3, config = Map.empty)
        val actualKafkaClusterChanges = createDeployChangesWithRedundantKafkaClusterItems(
          Map(deployed(topologyToDeploySourceSink(), notRemovedKeys = Seq(redundantTopic.key))),
          Seq(redundantTopic.withOwnerTopologyId(TopologyEntityId("test"))),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink() + (LocalTopicId("another-topic") --> TopologyToDeploy.Topic("test.another-topic", unManaged = true)))
        )

        actualKafkaClusterChanges shouldBe empty
      }
    }
  }

  "deploying a topic config change" when {
    "there are no other deployed topologies" should {
      "generates an UPDATE change only for the topic" in {
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          Map(deployed(topologyToDeploySourceSink())),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink() ~+ (LocalTopicId("topic") --> TopologyToDeploy.Topic("test.topic", config = Map("retention.ms" -> "61536000000"))))
        )

        val expectedKafkaClusterChanges =
          Map(
            KafkaClusterChange.UpdateTopic(
              KafkaClusterItem.Topic("test.topic", 1, 3, Map("retention.ms" -> "31536000000")),
              KafkaClusterItem.Topic("test.topic", 1, 3, Map("retention.ms" -> "61536000000"))
            ).toTuple
          )

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no other deployed topologies, but the topic doesn't exist in the cluster" should {
      "generates an ADD change for the topic" in {
        val nonExistingTopic = KafkaClusterItem.Topic("test.topic", 1, 3, Map("retention.ms" -> "31536000000"))
        val actualKafkaClusterChanges = createDeployChangesWithoutKafkaClusterItems(
          Map(deployed(topologyToDeploySourceSink())),
          Seq(nonExistingTopic).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink() ~+ (LocalTopicId("topic") --> TopologyToDeploy.Topic("test.topic", config = Map("retention.ms" -> "61536000000"))))
        )

        val expectedKafkaClusterChanges =
          Map(KafkaClusterChange.Add(KafkaClusterItem.Topic("test.topic", 1, 3, Map("retention.ms" -> "61536000000"))).toTuple)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no other deployed topologies, but the topic in the cluster has already the new config" should {
      "generates no changes" in {
        val actualKafkaClusterChanges = createDeployChangesWithDifferentKafkaClusterItems(
          Map(deployed(topologyToDeploySourceSink())),
          Seq(KafkaClusterItem.Topic("test.topic", config = Map("retention.ms" -> "61536000000"))).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink() ~+ (LocalTopicId("topic") --> TopologyToDeploy.Topic("test.topic", config = Map("retention.ms" -> "61536000000"))))
        )

        actualKafkaClusterChanges shouldBe empty
      }
    }

    "there are no other deployed topologies, but the topic in the cluster has a different config from the current and new state" should {
      "generates an UPDATE change only for the topic" in {
        val actualKafkaClusterChanges = createDeployChangesWithDifferentKafkaClusterItems(
          Map(deployed(topologyToDeploySourceSink())),
          Seq(KafkaClusterItem.Topic("test.topic", config = Map("retention.ms" -> "41536000000"))).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink() ~+ (LocalTopicId("topic") --> TopologyToDeploy.Topic("test.topic", config = Map("retention.ms" -> "61536000000"))))
        )

        val expectedKafkaClusterChanges =
          Map(
            KafkaClusterChange.UpdateTopic(
              KafkaClusterItem.Topic("test.topic", 1, 3, Map("retention.ms" -> "41536000000")),
              KafkaClusterItem.Topic("test.topic", 1, 3, Map("retention.ms" -> "61536000000"))
            ).toTuple
          )

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }
  }

  "deploying an un-managed topic config change" when {
    "there are no other deployed topologies" should {
      "generates no changes" in {
        val topology = topologyToDeploySourceSink().updateTopic("topic", topic => topic.copy(unManaged = true))
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          Map(deployed(topology)),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topology.updateTopic("topic", topic => topic.copy(config = topic.config + ("retention.ms" -> "61536000000"))))
        )

        actualKafkaClusterChanges shouldBe Map.empty
      }
    }

    "there are no other deployed topologies, but the topic doesn't exist in the cluster" should {
      "generates no changes" in {
        val topology = topologyToDeploySourceSink().updateTopic("topic", topic => topic.copy(unManaged = true))
        val nonExistingTopic = KafkaClusterItem.Topic("test.topic", 1, 3, Map("retention.ms" -> "31536000000"))
        val actualKafkaClusterChanges = createDeployChangesWithoutKafkaClusterItems(
          Map(deployed(topology)),
          Seq(nonExistingTopic).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topology.updateTopic("topic", topic => topic.copy(config = topic.config + ("retention.ms" -> "61536000000"))))
        )

        actualKafkaClusterChanges shouldBe Map.empty
      }
    }

    "there are no other deployed topologies, but the topic in the cluster has already the new config" should {
      "generates no changes" in {
        val topology = topologyToDeploySourceSink().updateTopic("topic", topic => topic.copy(unManaged = true))
        val actualKafkaClusterChanges = createDeployChangesWithDifferentKafkaClusterItems(
          Map(deployed(topology)),
          Seq(KafkaClusterItem.Topic("test.topic", config = Map("retention.ms" -> "61536000000"))).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topology.updateTopic("topic", topic => topic.copy(config = topic.config + ("retention.ms" -> "61536000000"))))
        )

        actualKafkaClusterChanges shouldBe empty
      }
    }

    "there are no other deployed topologies, but the topic in the cluster has a different config from the current and new state" should {
      "generates no changes" in {
        val topology = topologyToDeploySourceSink().updateTopic("topic", topic => topic.copy(unManaged = true))
        val actualKafkaClusterChanges = createDeployChangesWithDifferentKafkaClusterItems(
          Map(deployed(topology)),
          Seq(KafkaClusterItem.Topic("test.topic", config = Map("retention.ms" -> "41536000000"))).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topology.updateTopic("topic", topic => topic.copy(config = topic.config + ("retention.ms" -> "61536000000"))))
        )

        actualKafkaClusterChanges shouldBe empty
      }
    }
  }

  "deploying removing everything in the topology" when {
    "there are no other deployed topologies" should {
      "generates REMOVE changes for topics and ACLs" in {
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          Map(deployed(topologyToDeploySourceSink())),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(TopologyToDeploy())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are a few other deployed topologies" should {
      "generates REMOVE changes for topics and ACLs only for this topology" in {
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          Map(
            deployed(topologyToDeploySourceSink()),
            deployed(topologyToDeploySourceSink("test2"), "test2"),
            deployed(topologyToDeploySourceSink("test3"), "test3"),
            deployed(topologyToDeploySourceSink("test4"), "test4")
          ),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(TopologyToDeploy())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no other deployed topologies, but the topic already does not exist" should {
      "generates REMOVE changes for the ACLs only" in {
        val nonExistingTopic = KafkaClusterItems.forTopic(nullResolveReplicaPlacement)(topologyToDeploySourceSink().topic("topic").get).get
        val actualKafkaClusterChanges = createDeployChangesWithoutKafkaClusterItems(
          Map(deployed(topologyToDeploySourceSink())),
          Seq(nonExistingTopic).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(TopologyToDeploy())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .filterKeys(_ != nonExistingTopic.key)
            .mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no other deployed topologies, but the topic has a different config to the current" should {
      "generates REMOVE changes for topics and ACLs" in {
        val existingTopicWithDifferentConfig = KafkaClusterItem.Topic("test.topic", 1, 3, Map("retention.ms" -> "41536000000"))
        val actualKafkaClusterChanges = createDeployChangesWithDifferentKafkaClusterItems(
          Map(deployed(topologyToDeploySourceSink())),
          Seq(existingTopicWithDifferentConfig).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(TopologyToDeploy())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }
  }

  "deploying removing everything in the topology with an un-managed topic" when {
    "there are no other deployed topologies" should {
      "generates REMOVE changes for the ACLs only" in {
        val actualKafkaClusterChanges = createDeployChanges(
          Map(deployed(topologyToDeploySourceSink().updateTopic("topic", topic => topic.copy(unManaged = true)))),
          kafkaClusterItemOfTopologiesOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(TopologyToDeploy())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .collect {
              case (k, i: KafkaClusterItem.Topic) if i.name != "test.topic" => (k, i)
              case (k, i: KafkaClusterItem.Acl) => (k, i)
            }
            .mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no other deployed topologies, but the topic already does not exist" should {
      "generates REMOVE changes for the ACLs only" in {
        val nonExistingTopic = KafkaClusterItems.forTopic(nullResolveReplicaPlacement)(topologyToDeploySourceSink().topic("topic").get).get
        val actualKafkaClusterChanges = createDeployChanges(
          Map(deployed(topologyToDeploySourceSink().updateTopic("topic", topic => topic.copy(unManaged = true)))),
          kafkaClusterItemOfTopologiesOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .filter(i => i.kafkaClusterItem.key != nonExistingTopic.key),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(TopologyToDeploy())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .filterKeys(_ != nonExistingTopic.key)
            .mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no other deployed topologies, but the topic has a different config to the current" should {
      "generates REMOVE changes for the ACLs only" in {
        val actualKafkaClusterChanges = createDeployChanges(
          Map(deployed(topologyToDeploySourceSink().updateTopic("topic", topic => topic.copy(unManaged = true)))),
          kafkaClusterItemOfTopologiesOf(
            Map(deployed(topologyToDeploySourceSink()
              .updateTopic("topic", topic => topic.copy(config = topic.config + ("retention.ms" -> "41536000000"))))
            ),
            isKafkaClusterSecurityEnabled
          ),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(TopologyToDeploy())
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .collect {
              case (k, i: KafkaClusterItem.Topic) if i.name != "test.topic" => (k, i)
              case (k, i: KafkaClusterItem.Acl) => (k, i)
            }
            .mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }
  }

  "deploying a managed -> un-managed topic change" when {
    "there are no other deployed topologies" should {
      "generates no changes" in {
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          Map(deployed(topologyToDeploySourceSink())),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink().updateTopic("topic", topic => topic.copy(unManaged = true)))
        )

        actualKafkaClusterChanges shouldBe Map.empty
      }
    }
  }

  "deploying an un-managed -> managed topic change" when {
    "there are no other deployed topologies and the cluster does not contain the topic" should {
      "generates an ADD change for the topic" in {
        val existingTopology = TopologyToDeploy(Namespace("test"), TopologyId("1"), topics = Map("topic" -> TopologyToDeploy.Topic("test.topic", unManaged = true)).toMapByTopicId)
        val newTopology = existingTopology.updateTopic("topic", topic => topic.copy(unManaged = false))

        val actualKafkaClusterChanges = createDeployChanges(
          Map(deployed(existingTopology, existingTopology.topologyEntityId.id)),
          Seq.empty,
          isKafkaClusterSecurityEnabled,
          newTopology.topologyEntityId,
          Some(newTopology)
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(newTopology, newTopology.topologyEntityId.id)), isKafkaClusterSecurityEnabled)
            .mapValues(KafkaClusterChange.Add)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no other deployed topologies and the cluster contains the topic" should {
      "generates no changes" in {
        val actualKafkaClusterChanges = createDeployChanges(
          Map(deployed(topologyToDeploySourceSink().updateTopic("topic", topic => topic.copy(unManaged = true)))),
          kafkaClusterItemOfTopologiesOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          Some(topologyToDeploySourceSink())
        )

        actualKafkaClusterChanges shouldBe Map.empty
      }
    }
  }

  "un-deploying a topology" when {
    "there are no other deployed topologies" should {
      "generates REMOVE changes for topics and ACLs" in {
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          Map(deployed(topologyToDeploySourceSink())),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          None
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are a few other deployed topologies" should {
      "generates REMOVE changes for topics and ACLs only for this topology" in {
        val actualKafkaClusterChanges = createDeployChangesNoDiscrepancies(
          Map(
            deployed(topologyToDeploySourceSink()),
            deployed(topologyToDeploySourceSink("test2"), "test2"),
            deployed(topologyToDeploySourceSink("test3"), "test3"),
            deployed(topologyToDeploySourceSink("test4"), "test4")
          ),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          None
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no other deployed topologies, but the topic already does not exist" should {
      "generates REMOVE changes for the ACLs only" in {
        val nonExistingTopic = KafkaClusterItems.forTopic(nullResolveReplicaPlacement)(topologyToDeploySourceSink().topic("topic").get).get
        val actualKafkaClusterChanges = createDeployChangesWithoutKafkaClusterItems(
          Map(deployed(topologyToDeploySourceSink())),
          Seq(nonExistingTopic).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          None
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .filterKeys(_ != nonExistingTopic.key)
            .mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are a few other deployed topologies, and the topic already does not exist" should {
      "generates REMOVE changes for the ACLs only" in {
        val nonExistingTopic = KafkaClusterItems.forTopic(nullResolveReplicaPlacement)(topologyToDeploySourceSink().topic("topic").get).get
        val actualKafkaClusterChanges = createDeployChangesWithoutKafkaClusterItems(
          Map(
            deployed(topologyToDeploySourceSink()),
            deployed(topologyToDeploySourceSink("test2"), "test2"),
            deployed(topologyToDeploySourceSink("test3"), "test3"),
            deployed(topologyToDeploySourceSink("test4"), "test4")
          ),
          Seq(nonExistingTopic).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          None
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .filterKeys(_ != nonExistingTopic.key)
            .mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no other deployed topologies, but the topic has a different config to the current" should {
      "generates REMOVE changes for topics and ACLs" in {
        val existingTopicWithDifferentConfig = KafkaClusterItem.Topic("test.topic", 1, 3, Map("retention.ms" -> "41536000000"))
        val actualKafkaClusterChanges = createDeployChangesWithDifferentKafkaClusterItems(
          Map(deployed(topologyToDeploySourceSink())),
          Seq(existingTopicWithDifferentConfig).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          None
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are a few other deployed topologies, and the topic has a different config to the current" should {
      "generates REMOVE changes for topics and ACLs" in {
        val existingTopicWithDifferentConfig = KafkaClusterItem.Topic("test.topic", 1, 3, Map("retention.ms" -> "41536000000"))
        val actualKafkaClusterChanges = createDeployChangesWithDifferentKafkaClusterItems(
          Map(
            deployed(topologyToDeploySourceSink()),
            deployed(topologyToDeploySourceSink("test2"), "test2"),
            deployed(topologyToDeploySourceSink("test3"), "test3"),
            deployed(topologyToDeploySourceSink("test4"), "test4")
          ),
          Seq(existingTopicWithDifferentConfig).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          None
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there is another topology which needs an ACL from this topology" should {
      "does not generate REMOVE change for that ACLs" in {
        val sharedTopology = topologyToDeploySharedConsumableTopic()
        val topology1 = topologyToDeployConsumingSharedTopic("test1", user = Some("service-test"))
        val topology2 = topologyToDeployConsumingSharedTopic("test2", user = Some("service-test"))

        val existingTopicWithDifferentConfig = KafkaClusterItem.Topic("test.topic", 1, 3, Map("retention.ms" -> "41536000000"))
        val actualKafkaClusterChanges = createDeployChangesWithDifferentKafkaClusterItems(
          Map(
            deployed(sharedTopology, project = "shared"),
            deployed(topology1, "test1"),
            deployed(topology2, "test2")
          ),
          Seq(existingTopicWithDifferentConfig).map(_.withoutOwnerTopologyIds()),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test2"),
          None
        )

        val expectedKafkaClusterChanges =
          // only the group read ACL is removed, not the actual topic READ
          Map(KafkaClusterItem.Acl(ResourceType.GROUP, PatternType.LITERAL, "test2.sink", "User:service-test", "*", AclOperation.READ, AclPermissionType.ALLOW).toTuple)
            .mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }
  }

  "un-deploying a topology with an un-managed topic" when {
    "there are no other deployed topologies" should {
      "generates REMOVE changes for the ACLs only" in {
        val actualKafkaClusterChanges = createDeployChanges(
          Map(deployed(topologyToDeploySourceSink().updateTopic("topic", topic => topic.copy(unManaged = true)))),
          kafkaClusterItemOfTopologiesOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          None
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .collect {
              case (k, i: KafkaClusterItem.Topic) if i.name != "test.topic" => (k, i)
              case (k, i: KafkaClusterItem.Acl) => (k, i)
            }
            .mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no other deployed topologies, but the topic already does not exist" should {
      "generates REMOVE changes for the ACLs only" in {
        val nonExistingTopic = KafkaClusterItems.forTopic(nullResolveReplicaPlacement)(topologyToDeploySourceSink().topic("topic").get).get
        val actualKafkaClusterChanges = createDeployChanges(
          Map(deployed(topologyToDeploySourceSink().updateTopic("topic", topic => topic.copy(unManaged = true)))),
          kafkaClusterItemOfTopologiesOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .filter(i => i.kafkaClusterItem.key != nonExistingTopic.key),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          None
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .filterKeys(_ != nonExistingTopic.key)
            .mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }

    "there are no other deployed topologies, but the topic has a different config to the current" should {
      "generates REMOVE changes for the ACLs only" in {
        val actualKafkaClusterChanges = createDeployChanges(
          Map(deployed(topologyToDeploySourceSink().updateTopic("topic", topic => topic.copy(unManaged = true)))),
          kafkaClusterItemOfTopologiesOf(
            Map(deployed(topologyToDeploySourceSink()
              .updateTopic("topic", topic => topic.copy(config = topic.config + ("retention.ms" -> "41536000000"))))
            ),
            isKafkaClusterSecurityEnabled
          ),
          isKafkaClusterSecurityEnabled,
          TopologyEntityId("test"),
          None
        )

        val expectedKafkaClusterChanges =
          kafkaClusterItemsMapOf(Map(deployed(topologyToDeploySourceSink())), isKafkaClusterSecurityEnabled)
            .collect {
              case (k, i: KafkaClusterItem.Topic) if i.name != "test.topic" => (k, i)
              case (k, i: KafkaClusterItem.Acl) => (k, i)
            }
            .mapValues(KafkaClusterChange.Remove)

        actualKafkaClusterChanges shouldBe expectedKafkaClusterChanges
      }
    }
  }
}
