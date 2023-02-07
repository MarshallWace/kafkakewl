/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import com.mwam.kafkakewl.common.ValidationResultMatchers
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology._
import com.mwam.kafkakewl.domain.{TestTopologies, TestTopologiesCommon}
import org.scalatest.{FlatSpec, Matchers}

class TopologyValidatorStandaloneRelationshipsSpec extends FlatSpec
  with Matchers
  with ValidationResultMatchers
  with TestTopologies
  with TestTopologiesCommon
{
  val topicDefaults: TopicDefaults = TopicDefaults()

  def validateTopology(
    newTopologyId: TopologyEntityId,
    newTopology: Topology
  ): Validation.Result = TopologyValidator.validateTopology(Map.empty, newTopologyId, Some(newTopology), topicDefaults)

  "a topology with a relationship with a topic that is not defined" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      applications = Map("app" -> Topology.Application("service-test").makeSimple(Some("test.app"))).toMapByApplicationId,
      relationships = Map(
        relationshipFrom("app", (RelationshipType.Consume(), Seq(("topic", None))))
      )
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("could not find 'topic' for relationship")
  }

  "a topology with a relationship with an application that is not defined" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map("topic" -> Topology.Topic("test.topic")).toMapByTopicId,
      relationships = Map(
        relationshipFrom("app", (RelationshipType.Consume(), Seq(("topic", None))))
      )
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("could not find 'app' for relationship")
  }

  "a topology that has a connector application and a consume relationship" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map("topic1" -> Topology.Topic("test.topic1")).toMapByTopicId,
      applications = Map(
        "source-connector" -> Topology.Application("service-source").makeConnector("test-connector")
      ).toMapByApplicationId,
      relationships = Map(
        relationshipFrom("source-connector", (RelationshipType.Consume(), Seq(("test.topic1", None))))
      )
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("does not have a consumer group")
  }

  "a topology that has a connector producer application and a regex relationship for it" should "be valid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map(
        "topic1" -> Topology.Topic("test.topic1"),
        "topic2" -> Topology.Topic("test.topic2"),
        "topic3" -> Topology.Topic("test.topic3"),
        "topic4" -> Topology.Topic("test.topic4"),
        "topic5" -> Topology.Topic("test.topic5")
      ).toMapByTopicId,
      applications = Map("source-connector" -> Topology.Application("service-source").makeConnector("test-connector")).toMapByApplicationId,
      aliases = LocalAliases(topics = Map("topics" -> FlexibleNodeId.Regex("""test\.topic\d""")).toAliases),
      relationships = Map(
        relationshipFrom("source-connector", (RelationshipType.Produce(), Seq(("topics", None))))
      )
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beValid
  }

  "a topology that has an application and alias relationships for it with both local and fully qualified names" should "be valid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map(
        "topic-1" -> Topology.Topic("test.topic-1"),
        "topic-2" -> Topology.Topic("test.topic-2"),
        "topic-other-1" -> Topology.Topic("test.topic-other-1"),
        "topic-other-2" -> Topology.Topic("test.topic-other-2")
      ).toMapByTopicId,
      applications = Map("source" -> Topology.Application("test.source")).toMapByApplicationId,
      aliases = LocalAliases(
        topics = Map(
          "topics" -> FlexibleNodeId.Regex("""test\.topic\-\d"""),
          "other-topics" -> FlexibleNodeId.Regex("""test\.topic\-other\-\d""")
        ).toAliases
      ),
      relationships = Map(
        relationshipFrom("source", (RelationshipType.Produce(), Seq(("topics", None))), (RelationshipType.Produce(), Seq(("test.other-topics", None)))),
      )
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beValid
  }

  "a topology that has an application and alias relationships for it with both local and fully qualified names in the exact alias strings" should "be valid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map(
        "topic-1" -> Topology.Topic("test.topic-1"),
        "topic-2" -> Topology.Topic("test.topic-2"),
        "topic-other-1" -> Topology.Topic("test.topic-other-1"),
        "topic-other-2" -> Topology.Topic("test.topic-other-2")
      ).toMapByTopicId,
      applications = Map("source" -> Topology.Application("test.source")).toMapByApplicationId,
      aliases = LocalAliases(
        topics = Map(
          "topics" -> Seq(FlexibleNodeId.Exact("topic-1"), FlexibleNodeId.Exact("topic-2")),
          "other-topics" -> Seq(FlexibleNodeId.Exact("test.topic-other-1"), FlexibleNodeId.Exact("test.topic-other-2")),
        ).toAliases
      ),
      relationships = Map(
        relationshipFrom("source", (RelationshipType.Produce(), Seq(("topics", None))), (RelationshipType.Produce(), Seq(("other-topics", None)))),
      )
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beValid
  }

  "a topology that has an application and exact alias relationships for it with one missing" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map(
        "topic-1" -> Topology.Topic("test.topic-1"),
        "topic-2" -> Topology.Topic("test.topic-2")
      ).toMapByTopicId,
      applications = Map("source" -> Topology.Application("test.source")).toMapByApplicationId,
      aliases = LocalAliases(
        topics = Map(
          "topics" -> Seq(FlexibleNodeId.Exact("topic-1"), FlexibleNodeId.Exact("topic-3"))
        ).toAliases
      ),
      relationships = Map(
        relationshipFrom("source", (RelationshipType.Produce(), Seq(("topics", None))))
      )
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("could not find 'topic-3' for alias 'topics'")
  }

  "a topology that has a few applications and a regex relationship for them" should "be valid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map(
        "topic1" -> Topology.Topic("test.topic1"),
        "topic2" -> Topology.Topic("test.topic2")
      ).toMapByTopicId,
      applications = Map(
        "processor" -> Topology.Application("service-test").makeKafkaStreams("test.__kstreams__.Processor"),
        "custom-parser-1" -> Topology.Application("service-test").makeSimple(Some("test.custom-parser-1")),
        "custom-parser-2" -> Topology.Application("service-test").makeSimple(Some("test.custom-parser-2"))
      ).toMapByApplicationId,
      aliases = LocalAliases(
        topics = Map("topics" -> FlexibleNodeId.Prefix("test.")).toAliases,
        applications = Map("Test" -> FlexibleNodeId.Namespace("test")).toAliases
      ),
      relationships = Map(
        relationshipFrom(
          "Test",
          (RelationshipType.Produce(), Seq(("topics", None))),
          (RelationshipType.Consume(), Seq(("topics", None)))
        )
      )
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beValid
  }

  "a topology that has a connector producer application and a regex relationship for it but the regex does not match any topics" should "be valid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map(
        "topic1" -> Topology.Topic("test.topic1"),
        "topic2" -> Topology.Topic("test.topic2"),
        "topic3" -> Topology.Topic("test.topic3"),
        "topic4" -> Topology.Topic("test.topic4"),
        "topic5" -> Topology.Topic("test.topic5")
      ).toMapByTopicId,
      applications = Map("source-connector" -> Topology.Application("service-test").makeConnector("test-connector")).toMapByApplicationId,
      aliases = LocalAliases(
        topics = Map("topics" -> FlexibleNodeId.Regex("""test\.topic\d\d""")).toAliases,
      ),
      relationships = Map(
        relationshipFrom("source-connector", (RelationshipType.Produce(), Seq(("topics", None))))
      )
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beValid
  }

  s"a topology that has a custom relationship" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map("topic1" -> Topology.Topic("test.topic1")).toMapByTopicId,
      applications = Map("source" -> Topology.Application("service-test")).toMapByApplicationId,
      relationships = Map(
        relationshipFrom("source", (RelationshipType.Custom("custom"), Seq(("topic1", None))))
      )
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("not allowed relationship types: 'custom'")
  }

  s"a topology that has a produce relationship with monitorLag" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map("topic1" -> Topology.Topic("test.topic1")).toMapByTopicId,
      applications = Map(
        "source1" -> Topology.Application("service-test").makeSimple(),
        "source2" -> Topology.Application("service-test").makeSimple()
      ).toMapByApplicationId,
      relationships = Map(
        relationshipFrom("source1", (RelationshipType.Produce(), Seq(("topic1", Some(Topology.RelationshipProperties(monitorConsumerLag = Some(false))))))),
        relationshipFrom("source2", (RelationshipType.Produce(), Seq(("topic1", Some(Topology.RelationshipProperties(monitorConsumerLag = Some(true))))))),
      )
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessages(
      "'source1': monitorConsumerLag cannot be set for 'produce' 'topic1' relationship",
      "'source2': monitorConsumerLag cannot be set for 'produce' 'topic1' relationship"
    )
  }

  s"a topology that has identical relationships for the same application" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map("topic1" -> Topology.Topic("test.topic1")).toMapByTopicId,
      applications = Map("sink" -> Topology.Application("service-test").makeSimple(Some("test.sink"))).toMapByApplicationId,
      relationships = Map(
        relationshipFrom("sink", (RelationshipType.Consume(), Seq(("topic1", Some(Topology.RelationshipProperties(reset = Some(ResetMode.End))))))),
        relationshipFrom("test.sink", (RelationshipType.Consume(), Seq(("topic1", None))))
      )
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("duplicate relationships after resolving: 'test.sink'-consume-'test.topic1'")
  }

  s"a topology that has identical relationships for the same topic" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map("topic1" -> Topology.Topic("test.topic1")).toMapByTopicId,
      applications = Map("sink" -> Topology.Application("service-test").makeSimple(Some("test.sink"))).toMapByApplicationId,
      relationships = Map(
        relationshipFrom(
          "sink",
          (RelationshipType.Consume(), Seq(
            ("topic1", Some(Topology.RelationshipProperties(reset = Some(ResetMode.End)))),
            ("test.topic1", Some(Topology.RelationshipProperties(reset = Some(ResetMode.Beginning))))
          )),
        ),
      )
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("duplicate relationships after resolving: 'test.sink'-consume-'test.topic1'")
  }

  s"a topology that has identical relationships due to overlapping topic aliases" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map("topic1" -> Topology.Topic("test.topic1")).toMapByTopicId,
      applications = Map("sink" -> Topology.Application("service-test").makeSimple(Some("test.sink"))).toMapByApplicationId,
      aliases = LocalAliases(topics = Map("topics" -> FlexibleNodeId.Regex(""".*""")).toAliases),
      relationships = Map(
        relationshipFrom(
          "sink",
          (RelationshipType.Consume(), Seq(
            ("topic1", None),
            ("topics", None)
          )),
        ),
      )
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("duplicate relationships after resolving: 'test.sink'-consume-'test.topic1'")
  }

  s"a topology that has identical relationships due to overlapping application aliases" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map("topic1" -> Topology.Topic("test.topic1")).toMapByTopicId,
      applications = Map("sink" -> Topology.Application("service-test").makeSimple(Some("test.sink"))).toMapByApplicationId,
      aliases = LocalAliases(applications = Map("apps" -> FlexibleNodeId.Regex(""".*""")).toAliases),
      relationships = Map(
        relationshipFrom("sink", (RelationshipType.Consume(), Seq(("topic1", Some(Topology.RelationshipProperties(reset = Some(ResetMode.End))))))),
        relationshipFrom("apps", (RelationshipType.Consume(), Seq(("topic1", None))))
      )
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("duplicate relationships after resolving: 'test.sink'-consume-'test.topic1'")
  }

  s"a topology that has different relationships for the same application (local and fully-qualified names)" should "be valid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map("topic1" -> Topology.Topic("test.topic1")).toMapByTopicId,
      applications = Map("sink" -> Topology.Application("service-test").makeSimple(Some("test.sink"))).toMapByApplicationId,
      relationships = Map(
        relationshipFrom("sink", (RelationshipType.Consume(), Seq(("topic1", Some(Topology.RelationshipProperties(reset = Some(ResetMode.End))))))),
        relationshipFrom("test.sink", (RelationshipType.Produce(), Seq(("topic1", None))))
      )
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beValid
  }

  for (relationshipType <- List(RelationshipType.Consume(), RelationshipType.Produce())) yield {
    s"a topology that has a $relationshipType relationship from a topic to an application" should "be invalid" in {
      val topology = Topology(
        Namespace("test"),
        topics = Map("topic1" -> Topology.Topic("test.topic1")).toMapByTopicId,
        applications = Map("source" -> Topology.Application("service-test")).toMapByApplicationId,
        relationships = Map(
          relationshipFrom("topic1", (relationshipType, Seq(("source", None))))
        )
      )
      val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
      actualValidationResult should beInvalid
      actualValidationResult should containMessage("consume or produce relationships must start with an application but it was a topic: 'test.topic1'")
    }

    s"a topology that has a $relationshipType relationship from a topic to a topic" should "be invalid" in {
      val topology = Topology(
        Namespace("test"),
        topics = Map("topic1" -> Topology.Topic("test.topic1"), "topic2" -> Topology.Topic("test.topic2")).toMapByTopicId,
        applications = Map("source" -> Topology.Application("service-test")).toMapByApplicationId,
        relationships = Map(
          relationshipFrom("topic1", (relationshipType, Seq(("topic2", None))))
        )
      )
      val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
      actualValidationResult should beInvalid
      actualValidationResult should containMessage("consume or produce relationships must start with an application but it was a topic: 'test.topic1'")
    }
  }
}
