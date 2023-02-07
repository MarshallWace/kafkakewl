/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import com.mwam.kafkakewl.common.ValidationResultMatchers
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology._
import com.mwam.kafkakewl.domain.{FlexibleName, TestTopologies, TestTopologiesCommon}
import org.scalatest.{Matchers, WordSpec}

class TopologyValidatorDependentSpec extends WordSpec
  with Matchers
  with ValidationResultMatchers
  with TestTopologies
  with TestTopologiesCommon
{
  val topicDefaults: TopicDefaults = TopicDefaults()

  "a new topology consuming an external topic with an external application" when {
    "the other topology allows that topic to be consumed by that application" should {
      "be valid" in {
        val currentTopologies = Seq(
          topologySharedConsumableTopic("shared-topic"),
          topologySharedConsumingApplication("shared-application")
        ).toMapByTopologyId
        val newTopology =
          Topology(
            Namespace("test"),
            relationships = Map(
              relationshipFrom("shared-application.consuming-sink", (RelationshipType.Consume(), Seq(("shared-topic.topic-to-consume", None))))
            )
          )

        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), Some(newTopology), topicDefaults)
        actualValidationResult should beValid
      }
    }

    "the other topology does not allow that topic to be consumed by this namespace" should {
      "be invalid" in {
        val currentTopologies = Seq(
          topologySharedConsumableTopic("shared-topic") ~+ (LocalTopicId("topic-to-consume") --> Topology.Topic("shared-topic.topic-to-consume", otherConsumerNamespaces = Seq(FlexibleName.Namespace("shared-application")))),
          topologySharedConsumingApplication("shared-application")
        ).toMapByTopologyId
        val newTopology =
          Topology(
            Namespace("test"),
            relationships = Map(
              relationshipFrom("shared-application.consuming-sink", (RelationshipType.Consume(), Seq(("shared-topic.topic-to-consume", None))))
            )
          )

        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), Some(newTopology), topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("relationship's referenced topic 'shared-topic.topic-to-consume' cannot be consumed by applications in 'test' namespace")
      }
    }

    "the other topology does not allow that topic to be consumed by this namespace but it is a topic-default consumer namespace" should {
      "be valid" in {
        val currentTopologies = Seq(
          topologySharedConsumableTopic("shared-topic") ~+ (LocalTopicId("topic-to-consume") --> Topology.Topic("shared-topic.topic-to-consume", otherConsumerNamespaces = Seq(FlexibleName.Namespace("shared-application")))),
          topologySharedConsumingApplication("shared-application")
        ).toMapByTopologyId
        val newTopology =
          Topology(
            Namespace("test"),
            relationships = Map(
              relationshipFrom("shared-application.consuming-sink", (RelationshipType.Consume(), Seq(("shared-topic.topic-to-consume", None))))
            )
          )

        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), Some(newTopology), topicDefaults.withConsumerNamespace("test"))
        actualValidationResult should beValid
      }
    }

    "the other topology does not allow that topic to be consumed by the other namespace" should {
      "be invalid" in {
        val currentTopologies = Seq(
          topologySharedConsumableTopic("shared-topic") ~+ (LocalTopicId("topic-to-consume") --> Topology.Topic("shared.topic-to-consume", otherConsumerNamespaces = Seq(FlexibleName.Namespace("test")))),
          topologySharedConsumingApplication("shared-application")
        ).toMapByTopologyId
        val newTopology =
          Topology(
            Namespace("test"),
            relationships = Map(
              relationshipFrom("shared-application.consuming-sink", (RelationshipType.Consume(), Seq(("shared-topic.topic-to-consume", None))))
            )
          )

        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), Some(newTopology), topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("relationship's referenced topic 'shared-topic.topic-to-consume' cannot be consumed by applications in 'shared-application' namespace")
      }
    }

    "the other topology does not allow that topic to be consumed by the other namespace but it is a topic-default consumer namespace" should {
      "be valid" in {
        val currentTopologies = Seq(
          topologySharedConsumableTopic("shared-topic") ~+ (LocalTopicId("topic-to-consume") --> Topology.Topic("shared.topic-to-consume", otherConsumerNamespaces = Seq(FlexibleName.Namespace("test")))),
          topologySharedConsumingApplication("shared-application")
        ).toMapByTopologyId
        val newTopology =
          Topology(
            Namespace("test"),
            relationships = Map(
              relationshipFrom("shared-application.consuming-sink", (RelationshipType.Consume(), Seq(("shared-topic.topic-to-consume", None))))
            )
          )

        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), Some(newTopology), topicDefaults.withConsumerNamespace("shared-application"))
        actualValidationResult should beValid
      }
    }

    "the other topology does not allow that topic to be consumed by the either namespace" should {
      "be invalid" in {
        val currentTopologies = Seq(
          topologySharedConsumableTopic("shared-topic") ~+ (LocalTopicId("topic-to-consume") --> Topology.Topic("shared.topic-to-consume", otherConsumerNamespaces = Seq(FlexibleName.Namespace("other")))),
          topologySharedConsumingApplication("shared-application")
        ).toMapByTopologyId
        val newTopology =
          Topology(
            Namespace("test"),
            relationships = Map(
              relationshipFrom("shared-application.consuming-sink", (RelationshipType.Consume(), Seq(("shared-topic.topic-to-consume", None))))
            )
          )

        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), Some(newTopology), topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessages(
          "relationship's referenced topic 'shared-topic.topic-to-consume' cannot be consumed by applications in 'shared-application' namespace",
          "relationship's referenced topic 'shared-topic.topic-to-consume' cannot be consumed by applications in 'test' namespace")
      }
    }

    "the other topology does not allow that topic to be consumed by the either namespace but they are topic-default consumer namespaces" should {
      "be valid" in {
        val currentTopologies = Seq(
          topologySharedConsumableTopic("shared-topic") ~+ (LocalTopicId("topic-to-consume") --> Topology.Topic("shared.topic-to-consume", otherConsumerNamespaces = Seq(FlexibleName.Namespace("other")))),
          topologySharedConsumingApplication("shared-application")
        ).toMapByTopologyId
        val newTopology =
          Topology(
            Namespace("test"),
            relationships = Map(
              relationshipFrom("shared-application.consuming-sink", (RelationshipType.Consume(), Seq(("shared-topic.topic-to-consume", None))))
            )
          )

        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), Some(newTopology), topicDefaults.withConsumerNamespace("test", "shared-application"))
        actualValidationResult should beValid
      }
    }
  }

  "a new topology consuming an external topic" when {
    "the other topology allows that topic to be consumed" should {
      "be valid" in {
        val currentTopologies = Seq(topologySharedConsumableTopic()).toMapByTopologyId
        val newTopology = Some(topologyConsumingSharedTopic())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beValid
      }
    }

    "the other topology does not exist" should {
      "be invalid" in {
        val currentTopologies = Seq(topologySharedConsumableTopic("shared-other")).toMapByTopologyId
        val newTopology = Some(topologyConsumingSharedTopic())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("could not find 'shared.topic-to-consume' for relationship")
      }
    }

    "the other topology does not contain the topic to be consumed" should {
      "be invalid" in {
        val currentTopologies = Seq(Topology(Namespace("shared"))).toMapByTopologyId
        val newTopology = Some(topologyConsumingSharedTopic())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("could not find 'shared.topic-to-consume' for relationship")
      }
    }

    "the other topology does not allow the topic to be consumed at all" should {
      "be invalid" in {
        val currentTopologies = Seq(topologySharedConsumableTopic() ~+ (LocalTopicId("topic-to-consume") --> Topology.Topic("shared.topic-to-consume"))).toMapByTopologyId
        val newTopology = Some(topologyConsumingSharedTopic())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessages("relationship's referenced topic 'shared.topic-to-consume' cannot be consumed by applications in 'test' namespace")
      }
    }

    "the other topology does not allow the topic to be consumed at all but this namespace is topic-default consumer namespace" should {
      "be valid" in {
        val currentTopologies = Seq(topologySharedConsumableTopic() ~+ (LocalTopicId("topic-to-consume") --> Topology.Topic("shared.topic-to-consume"))).toMapByTopologyId
        val newTopology = Some(topologyConsumingSharedTopic())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults.withConsumerNamespace("test"))
        actualValidationResult should beValid
      }
    }

    "the other topology does not allow the regex topic to be consumed at all" should {
      "be invalid" in {
        val currentTopologies = Seq(
          topologySharedConsumableTopic() ~+ (LocalTopicId("topic-to-consume") --> Topology.Topic("shared.topic-to-consume", otherConsumerNamespaces = Seq(FlexibleName.Namespace("xyz"))))
        ).toMapByTopologyId
        val newTopology = Some(
          (topologyConsumingSharedTopic() + LocalAliases(topics = Map(LocalAliasId("shared-topics") -> Seq(FlexibleNodeId.Regex("""shared\.topic.*""")))))
            .withoutRelationshipOf(NodeRef("test.sink"))
            .withRelationship(relationshipFrom("test.sink", (RelationshipType.Consume(), Seq((s"shared-topics", None)))))
        )
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessages("cannot be consumed by applications in 'test' namespace")
      }
    }

    "the other topology does not allow the topic to be consumed by this application" should {
      "be invalid" in {
        val currentTopologies = Seq(
          topologySharedConsumableTopic() ~+ (LocalTopicId("topic-to-consume") --> Topology.Topic("shared.topic-to-consume", otherConsumerNamespaces = Seq(FlexibleName.Exact("test.some-other-namespace"))))
        ).toMapByTopologyId
        val newTopology = Some(topologyConsumingSharedTopic())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessages("cannot be consumed by applications in 'test' namespace")
      }
    }

    "the other topology does not allow the topic to be consumed by this application but this namespace is topic-default consumer namespace" should {
      "be valid" in {
        val currentTopologies = Seq(
          topologySharedConsumableTopic() ~+ (LocalTopicId("topic-to-consume") --> Topology.Topic("shared.topic-to-consume", otherConsumerNamespaces = Seq(FlexibleName.Exact("test.some-other-namespace"))))
        ).toMapByTopologyId
        val newTopology = Some(topologyConsumingSharedTopic())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults.withConsumerNamespace("test"))
        actualValidationResult should beValid
      }
    }

    "it has regex relationship matching the external topic" should {
      "be valid" in {
        val currentTopologies = Seq(topologySharedConsumableTopic()).toMapByTopologyId
        val newTopology = Topology(
          Namespace("shared"),
          TopologyId("other"),
          topics = Map(
            "local-topic1" -> Topology.Topic("shared.local-topic1"),
            "local-topic1" -> Topology.Topic("shared.local-topic2")
          ).toMapByTopicId,
          applications = Map("processor" -> Topology.Application("service-shared.processor").makeSimple(Some("shared.processor"))).toMapByApplicationId,
          aliases = LocalAliases(topics = Map(LocalAliasId("shared-topics") -> Seq(FlexibleNodeId.Regex("""shared\..*""")))),
          relationships = Map(
            relationshipFrom("processor", (RelationshipType.Consume(), Seq(("shared-topics", None))))
          )
        )
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("shared.other"), Some(newTopology), topicDefaults)
        actualValidationResult should beValid
      }
    }

    "it has regex relationship matching the external topic but it doesn't match any topics" should {
      "be valid" in {
        val currentTopologies = Seq(topologySharedConsumableTopic()).toMapByTopologyId
        val newTopology = Topology(
          Namespace("test"),
          applications = Map("processor" -> Topology.Application("service-test.processor")).toMapByApplicationId,
          aliases = LocalAliases(topics = Map(LocalAliasId("shared-local-topics") -> Seq(FlexibleNodeId.Regex("""shared\.local.*""")))),
          relationships = Map(
            relationshipFrom("processor", (RelationshipType.Consume(), Seq(("shared-local-topics", None))))
          )
        )
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), Some(newTopology), topicDefaults)
        actualValidationResult should beValid
      }
    }

    "it has an exact alias relationship to the external topic but it doesn't match any topics" should {
      "be valid" in {
        val currentTopologies = Seq(topologySharedConsumableTopic()).toMapByTopologyId
        val newTopology = Topology(
          Namespace("test"),
          applications = Map("processor" -> Topology.Application("service-test.processor")).toMapByApplicationId,
          aliases = LocalAliases(topics = Map(LocalAliasId("shared-local-topics") -> Seq(FlexibleNodeId.Exact("""shared.topic-to-consume-x""")))),
          relationships = Map(
            relationshipFrom("processor", (RelationshipType.Consume(), Seq(("shared-local-topics", None))))
          )
        )
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), Some(newTopology), topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("could not find 'shared.topic-to-consume-x' for alias 'shared-local-topics'")
      }
    }

    "it has an exact alias relationship to the external topic which exists but not consumable for this namespace" should {
      "be valid" in {
        val currentTopologies = Seq(
          topologySharedConsumableTopic() ~+ (LocalTopicId("topic-to-consume") --> Topology.Topic("shared.topic-to-consume", otherConsumerNamespaces = Seq(FlexibleName.Namespace("xyz"))))
        ).toMapByTopologyId
        val newTopology = Topology(
          Namespace("test"),
          applications = Map("processor" -> Topology.Application("service-test.processor")).toMapByApplicationId,
          aliases = LocalAliases(topics = Map(LocalAliasId("shared-local-topics") -> Seq(FlexibleNodeId.Exact("""shared.topic-to-consume""")))),
          relationships = Map(
            relationshipFrom("processor", (RelationshipType.Consume(), Seq(("shared-local-topics", None))))
          )
        )
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), Some(newTopology), topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("relationship's referenced topic 'shared.topic-to-consume' cannot be consumed by applications in 'test' namespace")
      }
    }
  }

  "a new topology producing an external topic" when {
    "the other topology allows that topic to be produced" should {
      "be valid" in {
        val currentTopologies = Seq(topologySharedProducableTopic()).toMapByTopologyId
        val newTopology = Some(topologyProducingSharedTopic())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beValid
      }
    }

    "the other topology does not exist" should {
      "be invalid" in {
        val currentTopologies = Seq(topologySharedProducableTopic("shared-other")).toMapByTopologyId
        val newTopology = Some(topologyProducingSharedTopic())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("could not find 'shared.topic-to-produce' for relationship")
      }
    }

    "the other topology does not contain the topic to be produced" should {
      "be invalid" in {
        val currentTopologies = Seq(Topology(Namespace("shared"))).toMapByTopologyId
        val newTopology = Some(topologyProducingSharedTopic())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("could not find 'shared.topic-to-produce' for relationship")
      }
    }

    "the other topology does not allow the topic to be produced at all" should {
      "be invalid" in {
        val currentTopologies = Seq(topologySharedProducableTopic() ~+ (LocalTopicId("topic-to-produce") --> Topology.Topic("shared.topic-to-produce"))).toMapByTopologyId
        val newTopology = Some(topologyProducingSharedTopic())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("relationship's referenced topic 'shared.topic-to-produce' cannot be produced by applications in 'test' namespace")
      }
    }

    "the other topology does not allow the topic to be produced at all but this namespace is topic-default consumer namespace" should {
      "be valid" in {
        val currentTopologies = Seq(topologySharedProducableTopic() ~+ (LocalTopicId("topic-to-produce") --> Topology.Topic("shared.topic-to-produce"))).toMapByTopologyId
        val newTopology = Some(topologyProducingSharedTopic())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults.withProducerNamespace("test"))
        actualValidationResult should beValid
      }
    }

    "the other topology does not allow the topic to be produced by this application" should {
      "be invalid" in {
        val currentTopologies = Seq(
          topologySharedProducableTopic() ~+ (LocalTopicId("topic-to-produce") --> Topology.Topic("shared.topic-to-produce", otherProducerNamespaces = Seq(FlexibleName.Exact("test.some-other-namespace"))))
        ).toMapByTopologyId
        val newTopology = Some(topologyProducingSharedTopic())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("relationship's referenced topic 'shared.topic-to-produce' cannot be produced by applications in 'test' namespace")
      }
    }
  }

  "a new topology making an external application to consume a topic" when {
    "the other topology allows that application to consume the topic" should {
      "be valid" in {
        val currentTopologies = Seq(topologySharedConsumingApplication()).toMapByTopologyId
        val newTopology = Some(topologyWithTopicConsumedBySharedConsumingApplication())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beValid
      }
    }

    "the other topology does not exist" should {
      "be invalid" in {
        val currentTopologies = Seq(topologySharedConsumingApplication("shared-other")).toMapByTopologyId
        val newTopology = Some(topologyWithTopicConsumedBySharedConsumingApplication())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("could not find 'shared.consuming-sink' for relationship")
      }
    }

    "the other topology does not contain the application to consume" should {
      "be invalid" in {
        val currentTopologies = Seq(Topology(Namespace("shared"))).toMapByTopologyId
        val newTopology = Some(topologyWithTopicConsumedBySharedConsumingApplication())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("could not find 'shared.consuming-sink' for relationship")
      }
    }

    "the other topology does not allow the topic to be consumed at all" should {
      "be invalid" in {
        val currentTopologies = Seq(topologySharedConsumingApplication() ~+ (LocalApplicationId("consuming-sink") --> Topology.Application("service-shared"))).toMapByTopologyId
        val newTopology = Some(topologyWithTopicConsumedBySharedConsumingApplication())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("relationship's referenced application 'shared.consuming-sink' cannot consume topics in 'test' namespace")
      }
    }

    "the other topology does not allow the topic to be consumed by this application" should {
      "be invalid" in {
        val sharedApplication = LocalApplicationId("consuming-sink") --> Topology.Application("service-shared", otherConsumableNamespaces = Seq(FlexibleName.Exact("test.some-other-app"))).makeSimple(Some("shared.consuming-sink"))
        val currentTopologies = Seq(topologySharedConsumingApplication() ~+ sharedApplication).toMapByTopologyId
        val newTopology = Some(topologyWithTopicConsumedBySharedConsumingApplication())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("relationship's referenced application 'shared.consuming-sink' cannot consume topics in 'test' namespace")
      }
    }
  }

  "a new topology making an external application to produce a topic" when {
    "the other topology allows that application to produce the topic" should {
      "be valid" in {
        val currentTopologies = Seq(topologySharedProducingApplication()).toMapByTopologyId
        val newTopology = Some(topologyWithTopicProducedBySharedProducingApplication())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beValid
      }
    }

    "the other topology does not exist" should {
      "be invalid" in {
        val currentTopologies = Seq(topologySharedProducingApplication("shared-other")).toMapByTopologyId
        val newTopology = Some(topologyWithTopicProducedBySharedProducingApplication())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("could not find 'shared.producing-source' for relationship")
      }
    }

    "the other topology does not contain the application to produce" should {
      "be invalid" in {
        val currentTopologies = Seq(Topology(Namespace("shared"))).toMapByTopologyId
        val newTopology = Some(topologyWithTopicProducedBySharedProducingApplication())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("could not find 'shared.producing-source' for relationship")
      }
    }

    "the other topology does not allow the topic to be produced at all" should {
      "be invalid" in {
        val currentTopologies = Seq(topologySharedProducingApplication() ~+ (LocalApplicationId("producing-source") --> Topology.Application("service-shared"))).toMapByTopologyId
        val newTopology = Some(topologyWithTopicProducedBySharedProducingApplication())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("relationship's referenced application 'shared.producing-source' cannot produce topics in 'test' namespace")
      }
    }

    "the other topology does not allow the topic to be produced by this application" should {
      "be invalid" in {
        val currentTopologies = Seq(
          topologySharedProducingApplication() ~+ (LocalApplicationId("producing-source") --> Topology.Application("service-shared", otherConsumableNamespaces = Seq(FlexibleName.Exact("test.some-other-app"))))
        ).toMapByTopologyId
        val newTopology = Some(topologyWithTopicProducedBySharedProducingApplication())
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("test"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("relationship's referenced application 'shared.producing-source' cannot produce topics in 'test' namespace")
      }
    }
  }

  "removing a consumable shared topic" when {
    "there are no topologies referencing it" should {
      "be valid" in {
        val currentTopologies = Seq(topologySharedConsumableTopic()).toMapByTopologyId
        val newTopology = Some(Topology(Namespace("shared")))
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("shared"), newTopology, topicDefaults)
        actualValidationResult should beValid
      }
    }

    "there are topologies referencing it but not with relationships" should {
      "be invalid" in {
        val currentTopologies = Seq(
          topologySharedConsumableTopic(),
          topologyConsumingSharedTopic().withoutRelationshipOf(NodeRef("test.sink"))
        ).toMapByTopologyId
        val newTopology = Some(Topology(Namespace("shared")))
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("shared"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("topology 'test' depends on topology 'shared': 'shared.topic-to-consume'")
      }
    }

    "there are topologies referencing it with consume relationship" should {
      "be invalid" in {
        val currentTopologies = Seq(
          topologySharedConsumableTopic(),
          topologyConsumingSharedTopic()
        ).toMapByTopologyId
        val newTopology = Some(Topology(Namespace("shared")))
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("shared"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("topology 'test' depends on topology 'shared': 'shared.topic-to-consume'")
      }
    }
  }

  "removing a producable shared topic" when {
    "there are no topologies referencing it" should {
      "be valid" in {
        val currentTopologies = Seq(topologySharedProducableTopic()).toMapByTopologyId
        val newTopology = Some(Topology(Namespace("shared")))
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("shared"), newTopology, topicDefaults)
        actualValidationResult should beValid
      }
    }

    "there are topologies referencing it but not with relationships" should {
      "be invalid" in {
        val currentTopologies = Seq(
          topologySharedProducableTopic(),
          topologyProducingSharedTopic().withoutRelationshipOf(NodeRef("test.source"))
        ).toMapByTopologyId
        val newTopology = Some(Topology(Namespace("shared")))
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("shared"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("topology 'test' depends on topology 'shared': 'shared.topic-to-produce'")
      }
    }

    "there are topologies referencing it with produce relationship" should {
      "be invalid" in {
        val currentTopologies = Seq(
          topologySharedProducableTopic(),
          topologyProducingSharedTopic()
        ).toMapByTopologyId
        val newTopology = Some(Topology(Namespace("shared")))
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("shared"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("topology 'test' depends on topology 'shared': 'shared.topic-to-produce'")
      }
    }
  }

  "emptying a consuming shared application" when {
    "there are no topologies referencing it" should {
      "be valid" in {
        val currentTopologies = Seq(topologySharedConsumingApplication()).toMapByTopologyId
        val newTopology = Some(Topology(Namespace("shared")))
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("shared"), newTopology, topicDefaults)
        actualValidationResult should beValid
      }
    }

    "there are topologies referencing it with consume relationship" should {
      "be invalid" in {
        val currentTopologies = Seq(
          topologySharedConsumingApplication(),
          topologyWithTopicConsumedBySharedConsumingApplication()
        ).toMapByTopologyId
        val newTopology = Some(Topology(Namespace("shared")))
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("shared"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("topology 'test' depends on topology 'shared': 'shared.consuming-sink'")
      }
    }
  }

  "removing a consuming shared application" when {
    "there are no topologies referencing it" should {
      "be valid" in {
        val currentTopologies = Seq(topologySharedConsumingApplication()).toMapByTopologyId
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("shared"), None, topicDefaults)
        actualValidationResult should beValid
      }
    }

    "there are topologies referencing it with consume relationship" should {
      "be invalid" in {
        val currentTopologies = Seq(
          topologySharedConsumingApplication(),
          topologyWithTopicConsumedBySharedConsumingApplication()
        ).toMapByTopologyId
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("shared"), None, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("topology 'test' depends on topology 'shared': 'shared.consuming-sink'")
      }
    }
  }

  "removing a producing shared application" when {
    "there are no topologies referencing it" should {
      "be valid" in {
        val currentTopologies = Seq(topologySharedProducingApplication()).toMapByTopologyId
        val newTopology = Some(Topology(Namespace("shared")))
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("shared"), newTopology, topicDefaults)
        actualValidationResult should beValid
      }
    }

    "there are topologies referencing it with consume relationship" should {
      "be invalid" in {
        val currentTopologies = Seq(
          topologySharedProducingApplication(),
          topologyWithTopicProducedBySharedProducingApplication()
        ).toMapByTopologyId
        val newTopology = Some(Topology(Namespace("shared")))
        val actualValidationResult = TopologyValidator.validateTopology(currentTopologies, TopologyEntityId("shared"), newTopology, topicDefaults)
        actualValidationResult should beInvalid
        actualValidationResult should containMessage("topology 'test' depends on topology 'shared': 'shared.producing-source'")
      }
    }
  }

  "a new simple standalone topology" when {
    "there is another identical topology" should {
      "be valid" in {
        val topology = topologySourceSink()
        val otherTopology = topology.copy(topology = TopologyId("other"))
        val actualValidationResult = TopologyValidator.validateTopology(Seq(otherTopology).toMapByTopologyId, TopologyEntityId("test"), Some(topology), topicDefaults)
        // it should be valid, because without deploying topologies, they can have identical topics, etc...
        actualValidationResult should beValid
      }
    }
  }

  "a new kafka-streams standalone topology" when {
    "there is another identical topology" should {
      "be valid" in {
        val topology = topologyKafkaStreams()
        val otherTopology = topology.copy(topology = TopologyId("other"))
        val actualValidationResult = TopologyValidator.validateTopology(Seq(otherTopology).toMapByTopologyId, TopologyEntityId("test"), Some(topology), topicDefaults)
        // it should be valid, because without deploying topologies, they can have identical topics, etc...
        actualValidationResult should beValid
      }
    }
  }

  "a new connector topology" when {
    "there is another identical topology" should {
      "be valid" in {
        val topology = topologyConnector()
        val otherTopology = topology.copy(topology = TopologyId("other"))
        val actualValidationResult = TopologyValidator.validateTopology(Seq(otherTopology).toMapByTopologyId, TopologyEntityId("test"), Some(topology), topicDefaults)
        // it should be valid, because without deploying topologies, they can have identical topics, etc...
        actualValidationResult should beValid
      }
    }
  }

  "a new connect-replicator topology" when {
    "there is another identical topology" should {
      "be valid" in {
        val topology = topologyConnectReplicator()
        val otherTopology = topology.copy(topology = TopologyId("other"))
        val actualValidationResult = TopologyValidator.validateTopology(Seq(otherTopology).toMapByTopologyId, TopologyEntityId("test"), Some(topology), topicDefaults)
        // it should be valid, because without deploying topologies, they can have identical topics, etc...
        actualValidationResult should beValid
      }
    }
  }
}
