/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import com.mwam.kafkakewl.common.topology.TopologyLikeOperations.{NodeRefResolveError, RelationshipNodeVisibilityError}
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology._
import com.mwam.kafkakewl.domain.{FlexibleName, TestTopologies, TestTopologiesCommon}
import org.scalatest.FlatSpec

class TopologyOperationsRelationshipsSpec
  extends FlatSpec
    with TopologyOperationsCommon
    with TestTopologies
    with TestTopologiesCommon
{
  val topicDefaults: TopicDefaults = TopicDefaults()

  "A simple topology with wild-card aliases" should "generate all the resolved relationships" in {
    val topology: Topology = Topology(
      Namespace("test"),
      topics = Map(
        "error" -> Topology.Topic("test.error"),
        "topic-1" -> Topology.Topic("test.topic-1"),
        "topic-2" -> Topology.Topic("test.topic-2"),
        "topic-3" -> Topology.Topic("test.topic-3"),
        "topic-4" -> Topology.Topic("test.topic-4"),
        "topic-other-1" -> Topology.Topic("test.topic-other-1"),
        "topic-other-2" -> Topology.Topic("test.topic-other-2")
      ).toMapByTopicId,
      applications = Map(
        "source" -> Topology.Application("service-test"),
        "sink" -> Topology.Application("service-test").makeSimple(Some("test.sink"), Some("test.sink")),
        "processor" -> Topology.Application("service-test").makeSimple(Some("test.processor"), Some("test.processor"))
      ).toMapByApplicationId,
      aliases = LocalAliases(
        topics = Map(
          "topic-x" -> Seq(FlexibleNodeId.Regex("""test\.topic-\d""")),
          "topic-no-match-x" -> Seq(FlexibleNodeId.Regex("""test\.topic-nomatch-\d""")),
          "topic-1-2-exact" -> Seq(FlexibleNodeId.Exact("topic-1"), FlexibleNodeId.Exact("topic-2")),
          "topic-4-5-exact" -> Seq(FlexibleNodeId.Exact("topic-4"), FlexibleNodeId.Exact("topic-5")),
        ).toAliases,
        applications = Map(
          "apps" -> FlexibleNodeId.Regex(""".*""")
        ).toAliases
      ),
      relationships = Map(
        relationshipFrom("source", (RelationshipType.Produce(), Seq(("topic-x", None)))),
        relationshipFrom("processor", (RelationshipType.Consume(), Seq(("topic-1-2-exact", None), ("topic-4-5-exact", None), ("test.other", None)))),
        relationshipFrom("sink", (RelationshipType.Consume(), Seq(("topic-x", None)))),
        relationshipFrom("apps", (RelationshipType.Produce(), Seq(("error", None))))
      )
    )

    val resolverFunc = resolveNodeRefFunc(allNodesMap(), topology)
    val (actualResolveErrors, actualVisibilityErrors, relationships) = collectAllVisibleRelationshipsFor(topology.topologyEntityId, topology: Topology, resolverFunc, topicDefaults)
    val expectedRelationships = Set(
      localProduceRelationship(topology, "source", node1Specific = true, "topic-1", node2Specific = false),
      localProduceRelationship(topology, "source", node1Specific = true, "topic-2", node2Specific = false),
      localProduceRelationship(topology, "source", node1Specific = true, "topic-3", node2Specific = false),
      localProduceRelationship(topology, "source", node1Specific = true, "topic-4", node2Specific = false),
      localConsumeRelationship(topology, "processor", node1Specific = true, "topic-1", node2Specific = true),
      localConsumeRelationship(topology, "processor", node1Specific = true, "topic-2", node2Specific = true),
      localConsumeRelationship(topology, "sink", node1Specific = true, "topic-1", node2Specific = false),
      localConsumeRelationship(topology, "sink", node1Specific = true, "topic-2", node2Specific = false),
      localConsumeRelationship(topology, "sink", node1Specific = true, "topic-3", node2Specific = false),
      localConsumeRelationship(topology, "sink", node1Specific = true, "topic-4", node2Specific = false),
      localProduceRelationship(topology, "source", node1Specific = false, "error", node2Specific = true),
      localProduceRelationship(topology, "sink", node1Specific = false, "error", node2Specific = true),
      localProduceRelationship(topology, "processor", node1Specific = false, "error", node2Specific = true),
    )
    val actualRelationships = relationships.toSet

    assert(actualRelationships == expectedRelationships)
    assert(actualResolveErrors.toSet == Set(
      NodeRefResolveError.DoesNotExist(NodeRef("test.other")),
      NodeRefResolveError.ExpectedFlexibleNodeIdNotFound(NodeRef("topic-4-5-exact"), Set("topic-5"))
    ))
    assert(actualVisibilityErrors.isEmpty)
  }

  "two simple topologies referencing each other with wild-card topics and applications" should "generate all the resolved relationships" in {
    val sharedTopology: Topology = Topology(
      Namespace("test"),
      TopologyId("shared"),
      topics = Map(
        "topic-1" -> Topology.Topic("test.shared.topic-1", otherConsumerNamespaces = Seq(FlexibleName.Any())),
        "topic-2" -> Topology.Topic("test.shared.topic-2", otherConsumerNamespaces = Seq(FlexibleName.Any())),
        "topic-for-md" -> Topology.Topic("test.shared.topic-for-md", otherConsumerNamespaces = Seq(FlexibleName.Namespace("xyz"))),
        "topic-other-for-md" -> Topology.Topic("test.shared.topic-other-for-md", otherConsumerNamespaces = Seq(FlexibleName.Namespace("xyz")))
      ).toMapByTopicId,
      applications = Map(
        "source" -> Topology.Application("service-test-shared"),
        "sink" -> Topology.Application("service-test-shared", otherConsumableNamespaces = Seq(FlexibleName.Any())).makeSimple(Some("test.shared.sink"), Some("test.shared.sink"))
      ).toMapByApplicationId,
      aliases = LocalAliases(
        topics = Map(
          "topic-x" -> FlexibleNodeId.Regex("""test\.shared\.topic-\d""")
        ).toAliases
      ),
      relationships = Map(
        relationshipFrom("source", (RelationshipType.Produce(), Seq(("topic-x", None)))),
      )
    )

    val topology: Topology = Topology(
      Namespace("test"),
      TopologyId("main"),
      topics = Map(
        "output.topic-1" -> Topology.Topic("test.main.output.topic-1"),
        "output.topic-2" -> Topology.Topic("test.main.output.topic-2")
      ).toMapByTopicId,
      applications = Map(
        "processor" -> Topology.Application("service-test").makeSimple(Some("test.main.processor"), Some("test.main.processor")),
        "processor-other" -> Topology.Application("service-test").makeSimple(Some("test.main.processor-other"), Some("test.main.processor-other")),
        "processor-all-shared" -> Topology.Application("service-test").makeSimple(Some("test.main.processor-all-shared"), Some("test.main.processor-all-shared"))
      ).toMapByApplicationId,
      aliases = LocalAliases(
        topics = Map(
          "shared.topics" -> Seq(FlexibleNodeId.Namespace("test.shared")),
          "shared.topic-x" -> Seq(FlexibleNodeId.Regex("""test\.shared\.topic-\d""")),
          "shared.topic-x-exact" -> Seq(FlexibleNodeId.Exact("test.shared.topic-1"), FlexibleNodeId.Exact("test.shared.topic-2")),
          "shared.topic-x-exact-missing" -> Seq(FlexibleNodeId.Exact("test.shared.topic-3")),
          "output.topic-x" -> Seq(FlexibleNodeId.Regex("""test\.main.output\.topic-\d"""))
        ).toAliases,
        applications = Map(
          "sinks" -> FlexibleNodeId.Regex(""".*\.sink"""),
          "processor-local" -> FlexibleNodeId.Regex("test.main.processor")
        ).toAliases
      ),
      relationships = Map(
        relationshipFrom("processor", (RelationshipType.Consume(), Seq(("shared.topic-x", None))), (RelationshipType.Produce(), Seq(("output.topic-x", None)))),
        relationshipFrom("processor-all-shared", (RelationshipType.Consume(), Seq(("shared.topics", None)))),
        relationshipFrom("processor-other",
          (RelationshipType.Consume(), Seq(
            ("shared.topic-x-exact", None),
            // this should generate a resolve error
            ("shared.topic-x-exact-missing", None),
            // this should generate a resolve error
            ("test.shared.topic-4", None),
            // this should generate a visibility error
            ("test.shared.topic-for-md", None)))),
        relationshipFrom("sinks", (RelationshipType.Consume(), Seq(("output.topic-x", None)))),
        // this should generate a visibility error
        relationshipFrom("processor-local", (RelationshipType.Consume(), Seq(("test.shared.topic-other-for-md", None)))),
      )
    )

    val resolverFunc = resolveNodeRefFunc(allNodesMap(sharedTopology), topology)
    val (actualResolveErrors, actualVisibilityErrors, relationships) = collectAllVisibleRelationshipsFor(topology.topologyEntityId, topology: Topology, resolverFunc, topicDefaults)
    val expectedRelationships = Set(
      consumeRelationship(topology, "processor", node1Specific = true, sharedTopology, "topic-1", node2Specific = false),
      consumeRelationship(topology, "processor", node1Specific = true, sharedTopology, "topic-2", node2Specific = false),
      produceRelationship(topology, "processor", node1Specific = true, topology, "output.topic-1", node2Specific = false),
      produceRelationship(topology, "processor", node1Specific = true, topology, "output.topic-2", node2Specific = false),
      consumeRelationship(topology, "processor-all-shared", node1Specific = true, sharedTopology, "topic-1", node2Specific = false),
      consumeRelationship(topology, "processor-all-shared", node1Specific = true, sharedTopology, "topic-2", node2Specific = false),
      consumeRelationship(topology, "processor-other", node1Specific = true, sharedTopology, "topic-1", node2Specific = true),
      consumeRelationship(topology, "processor-other", node1Specific = true, sharedTopology, "topic-2", node2Specific = true),
      consumeRelationship(sharedTopology, "sink", node1Specific = false, topology, "output.topic-1", node2Specific = false),
      consumeRelationship(sharedTopology, "sink", node1Specific = false, topology, "output.topic-2", node2Specific = false),
    )
    val actualRelationships = relationships.toSet

    assert(actualRelationships == expectedRelationships)
    assert(actualResolveErrors.toSet == Set(
      NodeRefResolveError.ExpectedFlexibleNodeIdNotFound(NodeRef("shared.topic-x-exact-missing"), Set("test.shared.topic-3")),
      NodeRefResolveError.DoesNotExist(NodeRef("test.shared.topic-4"))
    ))
    assert(actualVisibilityErrors.toSet == Set(
      RelationshipNodeVisibilityError.TopicCannotBeConsumedByNamespace(topicNode(sharedTopology, "topic-for-md"), Namespace("test.main")),
      RelationshipNodeVisibilityError.TopicCannotBeConsumedByNamespace(topicNode(sharedTopology, "topic-other-for-md"), Namespace("test.main"))
    ))
  }

  "two simple topologies referencing each other with non-standard relationships" should "generates errors" in {
    val sharedTopology: Topology = Topology(
      Namespace("test"),
      TopologyId("shared"),
      topics = Map(
        "topic-1" -> Topology.Topic("test.shared.topic-1", otherConsumerNamespaces = Seq(FlexibleName.Any())),
        "topic-2" -> Topology.Topic("test.shared.topic-2", otherConsumerNamespaces = Seq(FlexibleName.Any())),
        "topic-3" -> Topology.Topic("test.shared.topic-3", otherConsumerNamespaces = Seq(FlexibleName.Any()))
      ).toMapByTopicId,
      applications = Map(
        "sink-1" -> Topology.Application("service-test-shared", otherConsumableNamespaces = Seq(FlexibleName.Any())).makeSimple(Some("test.shared.sink-1"), Some("test.shared.sink-1")),
        "sink-2" -> Topology.Application("service-test-shared", otherConsumableNamespaces = Seq(FlexibleName.Any())).makeSimple(Some("test.shared.sink-2"), Some("test.shared.sink-2")),
        "sink-3" -> Topology.Application("service-test-shared", otherConsumableNamespaces = Seq(FlexibleName.Any())).makeSimple(Some("test.shared.sink-3"), Some("test.shared.sink-3"))
      ).toMapByApplicationId
    )

    val topology: Topology = Topology(
      Namespace("test"),
      TopologyId("main"),
      topics = Map(
        "output.topic-1" -> Topology.Topic("test.main.output.topic-1"),
        "output.topic-2" -> Topology.Topic("test.main.output.topic-2"),
        "output.topic-3" -> Topology.Topic("test.main.output.topic-3"),
        "output.topic-4" -> Topology.Topic("test.main.output.topic-4")
      ).toMapByTopicId,
      applications = Map(
        "processor" -> Topology.Application("service-test").makeSimple(Some("test.main.processor"), Some("test.main.processor")),
        "sink" -> Topology.Application("service-test").makeSimple(Some("test.main.sink"), Some("test.main.sink")),
      ).toMapByApplicationId,
      relationships = Map(
        // app - app relationships with both local -> fine
        relationshipFrom("sink", (RelationshipType.Consume(), Seq(("processor", None)))),
        // topic - app relationships with both local -> fine
        relationshipFrom("output.topic-3", (RelationshipType.Produce(), Seq(("sink", None)))),
        // topic - topic relationships with both local -> fine
        relationshipFrom("output.topic-4", (RelationshipType.Produce(), Seq(("output.topic-3", None)))),

        // app - app relationships with one being external -> error
        relationshipFrom("processor", (RelationshipType.Consume(), Seq(("test.shared.sink-1", None)))),
        relationshipFrom("test.shared.sink-2", (RelationshipType.Consume(), Seq(("processor", None)))),
        // topic - app relationships with one being external -> error
        relationshipFrom("output.topic-1", (RelationshipType.Consume(), Seq(("test.shared.sink-3", None)))),
        relationshipFrom("test.shared.topic-1", (RelationshipType.Consume(), Seq(("processor", None)))),
        // topic - topic relationships with one being external -> error
        relationshipFrom("output.topic-2", (RelationshipType.Consume(), Seq(("test.shared.topic-2", None)))),
        relationshipFrom("test.shared.topic-3", (RelationshipType.Consume(), Seq(("output.topic-2", None))))
      )
    )

    val resolverFunc = resolveNodeRefFunc(allNodesMap(sharedTopology), topology)
    val (actualResolveErrors, actualVisibilityErrors, relationships) = collectAllVisibleRelationshipsFor(topology.topologyEntityId, topology: Topology, resolverFunc, topicDefaults)
    val expectedRelationships = Set(
      localConsumeRelationship(topology, "sink", node1Specific = true, "processor", node2Specific = true),
      localProduceRelationship(topology, "output.topic-3", node1Specific = true, "sink", node2Specific = true),
      localProduceRelationship(topology, "output.topic-4", node1Specific = true, "output.topic-3", node2Specific = true),
    )
    val actualRelationships = relationships.toSet

    assert(actualRelationships == expectedRelationships)
    assert(actualResolveErrors.isEmpty)
    assert(actualVisibilityErrors.toSet == Set(
      RelationshipNodeVisibilityError.RelationshipCannotBeExternal(applicationNode(sharedTopology, "sink-1")),
      RelationshipNodeVisibilityError.RelationshipCannotBeExternal(applicationNode(sharedTopology, "sink-2")),
      RelationshipNodeVisibilityError.RelationshipCannotBeExternal(applicationNode(sharedTopology, "sink-3")),
      RelationshipNodeVisibilityError.RelationshipCannotBeExternal(topicNode(sharedTopology, "topic-1")),
      RelationshipNodeVisibilityError.RelationshipCannotBeExternal(topicNode(sharedTopology, "topic-2")),
      RelationshipNodeVisibilityError.RelationshipCannotBeExternal(topicNode(sharedTopology, "topic-3"))
    ))
  }

  "a topology has a relationship with boths ends external in two other topologies" should "generate all the resolved relationships" in {
    val sharedTopology1: Topology = Topology(
      Namespace("shared1"),
      topics = Map(
        "topic-1" -> Topology.Topic("shared1.topic-1", otherConsumerNamespaces = Seq(FlexibleName.Any())),
        "topic-2" -> Topology.Topic("shared1.topic-2", otherConsumerNamespaces = Seq(FlexibleName.Namespace("test"), FlexibleName.Namespace("shared2"))),
        "topic-3" -> Topology.Topic("shared1.topic-3", otherConsumerNamespaces = Seq(FlexibleName.Namespace("test"))),
        "topic-4" -> Topology.Topic("shared1.topic-4", otherConsumerNamespaces = Seq(FlexibleName.Namespace("shared2"))),
        "topic-5" -> Topology.Topic("shared1.topic-5", otherProducerNamespaces = Seq(FlexibleName.Any())),
        "topic-6" -> Topology.Topic("shared1.topic-6", otherProducerNamespaces = Seq(FlexibleName.Namespace("test"), FlexibleName.Namespace("shared2"))),
        "topic-7" -> Topology.Topic("shared1.topic-7", otherProducerNamespaces = Seq(FlexibleName.Namespace("test"))),
        "topic-8" -> Topology.Topic("shared1.topic-8", otherProducerNamespaces = Seq(FlexibleName.Namespace("shared2")))
      ).toMapByTopicId
    )

    val sharedTopology2: Topology = Topology(
      Namespace("shared2"),
      applications = Map(
        "sink-1" -> Topology.Application("service-test-shared", otherConsumableNamespaces = Seq(FlexibleName.Any())).makeSimple(Some("shared2.sink-1"), Some("shared2.sink-1")),
        "sink-2" -> Topology.Application("service-test-shared", otherConsumableNamespaces = Seq(FlexibleName.Namespace("test"), FlexibleName.Namespace("shared1"))).makeSimple(Some("shared2.sink-2"), Some("shared2.sink-2")),
        "sink-3" -> Topology.Application("service-test-shared", otherConsumableNamespaces = Seq(FlexibleName.Namespace("test"))).makeSimple(Some("shared2.sink-3"), Some("shared2.sink-3")),
        "sink-4" -> Topology.Application("service-test-shared", otherConsumableNamespaces = Seq(FlexibleName.Namespace("shared1"))).makeSimple(Some("shared2.sink-4"), Some("shared2.sink-4")),
        "sink-5" -> Topology.Application("service-test-shared", otherProducableNamespaces = Seq(FlexibleName.Any())).makeSimple(Some("shared2.sink-5"), Some("shared2.sink-5")),
        "sink-6" -> Topology.Application("service-test-shared", otherProducableNamespaces = Seq(FlexibleName.Namespace("test"), FlexibleName.Namespace("shared1"))).makeSimple(Some("shared2.sink-6"), Some("shared2.sink-6")),
        "sink-7" -> Topology.Application("service-test-shared", otherProducableNamespaces = Seq(FlexibleName.Namespace("test"))).makeSimple(Some("shared2.sink-7"), Some("shared2.sink-7")),
        "sink-8" -> Topology.Application("service-test-shared", otherProducableNamespaces = Seq(FlexibleName.Namespace("shared1"))).makeSimple(Some("shared2.sink-8"), Some("shared2.sink-8"))
      ).toMapByApplicationId
    )

    val topology: Topology = Topology(
      Namespace("test"),
      TopologyId("main"),
      relationships = Map(
        relationshipFrom("shared2.sink-1",
          (RelationshipType.Consume(), Seq(
            // these should be okay (both ends visible for anyone)
            ("shared1.topic-1", None),
            ("shared1.topic-2", None),
            // error: shared1.topic-3 is not consumable for shared2
            ("shared1.topic-3", None),
            // error: shared1.topic-4 is not consumable for test
            ("shared1.topic-4", None)
          ))),
        relationshipFrom("shared2.sink-2",
          (RelationshipType.Consume(), Seq(
            // these should be okay (both ends visible for anyone)
            ("shared1.topic-1", None),
            ("shared1.topic-2", None)
          ))),
        relationshipFrom("shared2.sink-3",
          (RelationshipType.Consume(), Seq(
            // error: shared2.sink-3 cannot consume shared1
            ("shared1.topic-1", None)
          ))),
        relationshipFrom("shared2.sink-4",
          (RelationshipType.Consume(), Seq(
            // error: shared2.sink-4 cannot consume test
            ("shared1.topic-1", None)
          ))),
        relationshipFrom("shared2.sink-5",
          (RelationshipType.Produce(), Seq(
            // these should be okay (both ends visible for anyone)
            ("shared1.topic-5", None),
            ("shared1.topic-6", None),
            // error: shared1.topic-7 is not producable for shared2
            ("shared1.topic-7", None),
            // error: shared1.topic-8 is not producable for test
            ("shared1.topic-8", None)
          ))),
        relationshipFrom("shared2.sink-6",
          (RelationshipType.Produce(), Seq(
            // these should be okay (both ends visible for anyone)
            ("shared1.topic-5", None),
            ("shared1.topic-6", None)
          ))),
        relationshipFrom("shared2.sink-7",
          (RelationshipType.Produce(), Seq(
            // error: shared2.sink-7 cannot produce shared1
            ("shared1.topic-5", None)
          ))),
        relationshipFrom("shared2.sink-8",
          (RelationshipType.Produce(), Seq(
            // error: shared2.sink-8 cannot produce test
            ("shared1.topic-5", None)
          )))
      )
    )

    val resolverFunc = resolveNodeRefFunc(allNodesMap(sharedTopology1, sharedTopology2), topology)
    val (actualResolveErrors, actualVisibilityErrors, relationships) = collectAllVisibleRelationshipsFor(topology.topologyEntityId, topology: Topology, resolverFunc, topicDefaults)
    val expectedRelationships = Set(
      consumeRelationship(sharedTopology2, "sink-1", node1Specific = true, sharedTopology1, "topic-1", node2Specific = true),
      consumeRelationship(sharedTopology2, "sink-1", node1Specific = true, sharedTopology1, "topic-2", node2Specific = true),
      consumeRelationship(sharedTopology2, "sink-2", node1Specific = true, sharedTopology1, "topic-1", node2Specific = true),
      consumeRelationship(sharedTopology2, "sink-2", node1Specific = true, sharedTopology1, "topic-2", node2Specific = true),
      produceRelationship(sharedTopology2, "sink-5", node1Specific = true, sharedTopology1, "topic-5", node2Specific = true),
      produceRelationship(sharedTopology2, "sink-5", node1Specific = true, sharedTopology1, "topic-6", node2Specific = true),
      produceRelationship(sharedTopology2, "sink-6", node1Specific = true, sharedTopology1, "topic-5", node2Specific = true),
      produceRelationship(sharedTopology2, "sink-6", node1Specific = true, sharedTopology1, "topic-6", node2Specific = true)
    )
    val actualRelationships = relationships.toSet

    assert(actualRelationships == expectedRelationships)
    assert(actualResolveErrors.isEmpty)
    assert(actualVisibilityErrors.toSet == Set(
      RelationshipNodeVisibilityError.TopicCannotBeConsumedByNamespace(topicNode(sharedTopology1, "topic-3"), Namespace("shared2")),
      RelationshipNodeVisibilityError.TopicCannotBeConsumedByNamespace(topicNode(sharedTopology1, "topic-4"), Namespace("test.main")),
      RelationshipNodeVisibilityError.ApplicationCannotConsumeNamespace(applicationNode(sharedTopology2, "sink-3"), Namespace("shared1")),
      RelationshipNodeVisibilityError.ApplicationCannotConsumeNamespace(applicationNode(sharedTopology2, "sink-4"), Namespace("test.main")),
      RelationshipNodeVisibilityError.TopicCannotBeProducedByNamespace(topicNode(sharedTopology1, "topic-7"), Namespace("shared2")),
      RelationshipNodeVisibilityError.TopicCannotBeProducedByNamespace(topicNode(sharedTopology1, "topic-8"), Namespace("test.main")),
      RelationshipNodeVisibilityError.ApplicationCannotProduceNamespace(applicationNode(sharedTopology2, "sink-7"), Namespace("shared1")),
      RelationshipNodeVisibilityError.ApplicationCannotProduceNamespace(applicationNode(sharedTopology2, "sink-8"), Namespace("test.main"))
    ))
  }
}
