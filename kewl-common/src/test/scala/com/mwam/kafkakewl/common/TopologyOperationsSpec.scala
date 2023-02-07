/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import com.mwam.kafkakewl.common.topology.TopologyLikeOperations.NodeRefResolveError
import com.mwam.kafkakewl.domain.topology._
import com.mwam.kafkakewl.domain.{TestTopologies, TestTopologiesCommon}
import org.scalatest.FlatSpec

class TopologyOperationsSpec
  extends FlatSpec
    with TopologyOperationsCommon
    with TestTopologies
    with TestTopologiesCommon {

  "A simple source-sink topology's topics and applications" should "resolve correctly" in {
    val topology = topologySourceSink()
    val resolverFunc = resolveNodeRefFunc(allNodesMap(topology), topology)

    // fully-qualified
    assert(resolverFunc(NodeRef("test.topic")).contains(Right(Iterable(topicNode(topology, "topic")))))
    assert(resolverFunc(NodeRef("test.source")).contains(Right(Iterable(applicationNode(topology, "source")))))
    assert(resolverFunc(NodeRef("test.sink")).contains(Right(Iterable(applicationNode(topology, "sink")))))
    // local
    assert(resolverFunc(NodeRef("topic")).contains(Right(Iterable(topicNode(topology, "topic")))))
    assert(resolverFunc(NodeRef("source")).contains(Right(Iterable(applicationNode(topology, "source")))))
    assert(resolverFunc(NodeRef("sink")).contains(Right(Iterable(applicationNode(topology, "sink")))))
    // something that doesn't exist
    assert(resolverFunc(NodeRef("test.other")).isEmpty)
  }

  "A simple source-sink topology's topics and applications" should "resolve correctly even if the nodes-map does not contain the current topology" in {
    val topology = topologySourceSink()
    val resolverFunc = resolveNodeRefFunc(allNodesMap(), topology)

    // fully-qualified
    assert(resolverFunc(NodeRef("test.topic")).contains(Right(Iterable(topicNode(topology, "topic")))))
    assert(resolverFunc(NodeRef("test.source")).contains(Right(Iterable(applicationNode(topology, "source")))))
    assert(resolverFunc(NodeRef("test.sink")).contains(Right(Iterable(applicationNode(topology, "sink")))))
    // local
    assert(resolverFunc(NodeRef("topic")).contains(Right(Iterable(topicNode(topology, "topic")))))
    assert(resolverFunc(NodeRef("source")).contains(Right(Iterable(applicationNode(topology, "source")))))
    assert(resolverFunc(NodeRef("sink")).contains(Right(Iterable(applicationNode(topology, "sink")))))
    // something that doesn't exist
    assert(resolverFunc(NodeRef("test.other")).isEmpty)
  }

  "A shared-topic consuming topology topics and applications" should "resolve correctly" in {
    val sharedTopology = topologySharedConsumableTopic()
    val topology = topologyConsumingSharedTopic()
    val resolverFunc = resolveNodeRefFunc(allNodesMap(sharedTopology, topology), topology)

    assert(resolverFunc(NodeRef("shared.topic-to-consume")).contains(Right(Iterable(topicNode(sharedTopology, "topic-to-consume")))))
    assert(resolverFunc(NodeRef("test.sink")).contains(Right(Iterable(applicationNode(topology, "sink")))))
    assert(resolverFunc(NodeRef("topic-to-consume")).isEmpty)
    assert(resolverFunc(NodeRef("test.topic-to-consume")).isEmpty)
    assert(resolverFunc(NodeRef("test.other")).isEmpty)
  }

  "A shared-topic consuming topology topics and applications" should "resolve correctly even if the nodes-map does not contain the current topology" in {
    val sharedTopology = topologySharedConsumableTopic()
    val topology = topologyConsumingSharedTopic()
    val resolverFunc = resolveNodeRefFunc(allNodesMap(sharedTopology), topology)

    assert(resolverFunc(NodeRef("shared.topic-to-consume")).contains(Right(Iterable(topicNode(sharedTopology, "topic-to-consume")))))
    assert(resolverFunc(NodeRef("test.sink")).contains(Right(Iterable(applicationNode(topology, "sink")))))
    assert(resolverFunc(NodeRef("topic-to-consume")).isEmpty)
    assert(resolverFunc(NodeRef("test.topic-to-consume")).isEmpty)
    assert(resolverFunc(NodeRef("test.other")).isEmpty)
  }

  "A simple topology with wild-card aliases" should "resolve correctly" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map(
        "topic-1" -> Topology.Topic(s"test.topic-1"),
        "topic-2" -> Topology.Topic(s"test.topic-2"),
        "topic-3" -> Topology.Topic(s"test.topic-3"),
        "topic-4" -> Topology.Topic(s"test.topic-4"),
        "topic-other-1" -> Topology.Topic(s"test.topic-other-1"),
        "topic-other-2" -> Topology.Topic(s"test.topic-other-2")
      ).toMapByTopicId,
      applications = Map(
        "source" -> Topology.Application(s"service-test"),
        "sink" -> Topology.Application(s"service-test").makeSimple(Some(s"test.sink"), Some(s"test.sink"))
      ).toMapByApplicationId,
      aliases = LocalAliases(
        topics = Map(
          "topic-x" -> Seq(FlexibleNodeId.Regex("""test\.topic-\d""")),
          "topic-x-exact" -> Seq(FlexibleNodeId.Exact("""test.topic-1"""), FlexibleNodeId.Exact("""topic-2"""), FlexibleNodeId.Exact("""topic-3""")),
          "topic-no-match" -> Seq(FlexibleNodeId.Regex("""test\.topic-nomatch-\d""")),
          "topic-no-match-exact" -> Seq(FlexibleNodeId.Exact("""topic-4"""), FlexibleNodeId.Exact("topic-5")),
          "topic-no-match-exact-regex" -> Seq(FlexibleNodeId.Regex("""test\.topic-\d"""), FlexibleNodeId.Exact("""topic-4"""), FlexibleNodeId.Exact("topic-5")),
          "topic-x-duplicate" -> Seq(FlexibleNodeId.Regex("""test\.topic-\d"""), FlexibleNodeId.Regex("""test\.topic-\d""")),
          "topic-no-match-x-duplicate" -> Seq(FlexibleNodeId.Regex("""test\.topic-nomatch-\d"""), FlexibleNodeId.Regex("""test\.topic-nomatch-\d"""))
        ).toAliases,
        applications = Map(
          "apps-exact" -> Seq(FlexibleNodeId.Exact("source"), FlexibleNodeId.Exact("sink")),
          "apps-regex" -> Seq(FlexibleNodeId.Regex(""".*""")),
          "apps-prefix" -> Seq(FlexibleNodeId.Prefix("test.")),
          "apps-namespace" -> Seq(FlexibleNodeId.Namespace("test"))
        ).toAliases
      )
    )

    val resolverFunc = resolveNodeRefFunc(allNodesMap(), topology)
    assert(resolverFunc(NodeRef("test.topic-1")).contains(Right(Iterable(topicNode(topology, "topic-1")))))
    assert(resolverFunc(NodeRef("test.topic-2")).contains(Right(Iterable(topicNode(topology, "topic-2")))))
    assert(resolverFunc(NodeRef("test.topic-3")).contains(Right(Iterable(topicNode(topology, "topic-3")))))
    assert(resolverFunc(NodeRef("test.topic-4")).contains(Right(Iterable(topicNode(topology, "topic-4")))))
    assert(resolverFunc(NodeRef("test.topic-5")).isEmpty)
    assert(resolverFunc(NodeRef("test.topic-x")).contains(Right(Iterable(
      topicNode(topology, "topic-1", specific = false),
      topicNode(topology, "topic-2", specific = false),
      topicNode(topology, "topic-3", specific = false),
      topicNode(topology, "topic-4", specific = false)))))
    assert(resolverFunc(NodeRef("test.topic-x-exact")).contains(Right(Iterable(
      topicNode(topology, "topic-1"),
      topicNode(topology, "topic-2"),
      topicNode(topology, "topic-3")))))
    assert(resolverFunc(NodeRef("test.topic-other-1")).contains(Right(Iterable(topicNode(topology, "topic-other-1")))))
    assert(resolverFunc(NodeRef("test.topic-other-2")).contains(Right(Iterable(topicNode(topology, "topic-other-2")))))
    assert(resolverFunc(NodeRef("test.topic-other-3")).isEmpty)
    assert(resolverFunc(NodeRef("test.topic-no-match")).contains(Right(Iterable.empty)))
    assert(resolverFunc(NodeRef("test.topic-no-match-exact")).contains(Left(NodeRefResolveError.ExpectedFlexibleNodeIdNotFound(NodeRef("test.topic-no-match-exact"), Set("topic-5")))))
    assert(resolverFunc(NodeRef("test.topic-no-match-exact-regex")).contains(Left(NodeRefResolveError.ExpectedFlexibleNodeIdNotFound(NodeRef("test.topic-no-match-exact-regex"), Set("topic-5")))))
    assert(resolverFunc(NodeRef("test.topic-x-duplicate")).contains(Right(Iterable(
      topicNode(topology, "topic-1", specific = false),
      topicNode(topology, "topic-2", specific = false),
      topicNode(topology, "topic-3", specific = false),
      topicNode(topology, "topic-4", specific = false)))))
    assert(resolverFunc(NodeRef("test.topic-no-match-x-duplicate")).contains(Right(Iterable.empty)))

    assert(resolverFunc(NodeRef("test.apps-exact")).contains(Right(Iterable(
      applicationNode(topology, "sink"),
      applicationNode(topology, "source")))))
    assert(resolverFunc(NodeRef("test.apps-regex")).contains(Right(Iterable(
      applicationNode(topology, "sink", specific = false),
      applicationNode(topology, "source", specific = false)))))
    assert(resolverFunc(NodeRef("test.apps-prefix")).contains(Right(Iterable(
      applicationNode(topology, "sink", specific = false),
      applicationNode(topology, "source", specific = false)))))
    assert(resolverFunc(NodeRef("test.apps-namespace")).contains(Right(Iterable(
      applicationNode(topology, "sink", specific = false),
      applicationNode(topology, "source", specific = false)))))
  }

  "A shared-topic consuming topology topics and applications with aliases" should "resolve correctly" in {
    val sharedTopology = topologySharedConsumableTopic()
    val topology = Topology(
      Namespace("test"),
      topics = Map(
        "topic-1" -> Topology.Topic(s"test.topic-1"),
        "topic-2" -> Topology.Topic(s"test.topic-2"),
        "topic-3" -> Topology.Topic(s"test.topic-3"),
        "topic-4" -> Topology.Topic(s"test.topic-4"),
        "topic-other-1" -> Topology.Topic(s"test.topic-other-1"),
        "topic-other-2" -> Topology.Topic(s"test.topic-other-2")
      ).toMapByTopicId,
      applications = Map(
        s"source" -> Topology.Application(s"service-test"),
        s"sink" -> Topology.Application(s"service-test").makeSimple(Some(s"test.sink"), Some(s"test.sink"))
      ).toMapByApplicationId,
      aliases = LocalAliases(
        topics = Map(
          "shared-topic-exact" -> Seq(FlexibleNodeId.Exact("shared.topic-to-consume")),
          "shared-topic-exact-no-match" -> Seq(FlexibleNodeId.Exact("shared.topic-to-consume-x")),
        ).toAliases
      )
    )
    val resolverFunc = resolveNodeRefFunc(allNodesMap(sharedTopology), topology)

    assert(resolverFunc(NodeRef("shared.topic-to-consume")).contains(Right(Iterable(topicNode(sharedTopology, "topic-to-consume")))))
    assert(resolverFunc(NodeRef("shared-topic-exact")).contains(Right(Iterable(topicNode(sharedTopology, "topic-to-consume")))))
    assert(resolverFunc(NodeRef("test.shared-topic-exact")).contains(Right(Iterable(topicNode(sharedTopology, "topic-to-consume")))))
    assert(resolverFunc(NodeRef("test.shared-topic-exact-no-match")).contains(Left(NodeRefResolveError.ExpectedFlexibleNodeIdNotFound(NodeRef("test.shared-topic-exact-no-match"), Set("shared.topic-to-consume-x")))))
  }
}
