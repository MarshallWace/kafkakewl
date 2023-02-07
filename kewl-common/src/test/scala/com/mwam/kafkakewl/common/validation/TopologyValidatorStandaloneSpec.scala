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
import org.scalatest.{FlatSpec, Matchers}

class TopologyValidatorStandaloneSpec extends FlatSpec
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

  "topology with empty namespace and empty topology" should "be invalid" in {
    val actualValidationResult = validateTopology(TopologyEntityId("test"), Topology())
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("at least one of the namespace or topology must be specified")
  }

  "topology with mismatching topology id" should "be invalid" in {
    val actualValidationResult = validateTopology(TopologyEntityId("test"), Topology(Namespace("ns")))
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("the topology's id ('ns') cannot be different from 'test'")
  }

  "topology with illegal namespace" should "be invalid" in {
    for (
      actualValidationResult <- Seq(
        validateTopology(TopologyEntityId("."), Topology(Namespace("."))),
        validateTopology(TopologyEntityId(".project"), Topology(Namespace(".project"))),
        validateTopology(TopologyEntityId("project."), Topology(Namespace("project."))),
        validateTopology(TopologyEntityId("project..package"), Topology(Namespace("project..package"))),
        validateTopology(TopologyEntityId("project "), Topology(Namespace("project "))),
        validateTopology(TopologyEntityId("project+"), Topology(Namespace("project+"))),
        validateTopology(TopologyEntityId("project/"), Topology(Namespace("project/"))),
        validateTopology(TopologyEntityId("project\\"), Topology(Namespace("project\\"))),
      )
    ) {
      actualValidationResult should beInvalid
      actualValidationResult should containMessage("must not start or end with '.', '_', '_' and can contain only alphanumeric characters and '.', '-', '_'")
    }
  }

  "topology with legal namespace" should "be invalid" in {
    for (
      actualValidationResult <- Seq(
        validateTopology(TopologyEntityId("project"), Topology(Namespace(""), TopologyId("project"))),
        validateTopology(TopologyEntityId("project"), Topology(Namespace("project"))),
        validateTopology(TopologyEntityId("project.package"), Topology(Namespace("project.package"))),
        validateTopology(TopologyEntityId("project-2._package_"), Topology(Namespace("project-2._package_"))),
      )
    ) {
      actualValidationResult should beValid
    }
  }

  "topology with illegal topology" should "be invalid" in {
    for (
      actualValidationResult <- Seq(
        validateTopology(TopologyEntityId("."), Topology(topology = TopologyId("."))),
        validateTopology(TopologyEntityId(".project"), Topology(topology = TopologyId(".project"))),
        validateTopology(TopologyEntityId("project."), Topology(topology = TopologyId("project."))),
        validateTopology(TopologyEntityId("project..package"), Topology(topology = TopologyId("project..package"))),
        validateTopology(TopologyEntityId("project "), Topology(topology = TopologyId("project "))),
        validateTopology(TopologyEntityId("project+"), Topology(topology = TopologyId("project+"))),
        validateTopology(TopologyEntityId("project/"), Topology(topology = TopologyId("project/"))),
        validateTopology(TopologyEntityId("project\\"), Topology(topology = TopologyId("project\\"))),
      )
    ) {
      actualValidationResult should beInvalid
      actualValidationResult should containMessage("must not start or end with '.', '_', '_' and can contain only alphanumeric characters and '.', '-', '_'")
    }
  }

  "topology with legal topology" should "be valid" in {
    for (
      actualValidationResult <- Seq(
        validateTopology(TopologyEntityId("project"), Topology(Namespace("project"), TopologyId(""))),
        validateTopology(TopologyEntityId("project"), Topology(topology = TopologyId("project"))),
        validateTopology(TopologyEntityId("project.package"), Topology(topology = TopologyId("project.package"))),
        validateTopology(TopologyEntityId("project-2._package_"), Topology(topology = TopologyId("project-2._package_"))),
      )
    ) {
      actualValidationResult should beValid
    }
  }

  "topology with legal namespace and topology" should "be valid" in {
    for (
      actualValidationResult <- Seq(
        validateTopology(TopologyEntityId("project.package"), Topology(Namespace("project"), TopologyId("package")))
      )
    ) {
      actualValidationResult should beValid
    }
  }

  "topology with a topic id with a dot" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map("test.topic1" -> Topology.Topic("test.topic1")).toMapByTopicId
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("topic id 'test.topic1' can contain only alphanumeric characters and '-', '_'")
  }

  "topology with a topic id with a slash" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map("test/topic1" -> Topology.Topic("test.topic1")).toMapByTopicId
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("topic id 'test/topic1' can contain only alphanumeric characters and '-', '_'")
  }

  "topology with a topic id with an asterisk" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map("test*topic1" -> Topology.Topic("test.topic1")).toMapByTopicId
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("topic id 'test*topic1' can contain only alphanumeric characters and '-', '_'")
  }

  "topology with an application id with a dot" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      applications = Map("test.app" -> Topology.Application("service-test")).toMapByApplicationId
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("application id 'test.app' can contain only alphanumeric characters and '-', '_'")
  }

  "topology with a application id with a slash" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      applications = Map("test/app" -> Topology.Application("service-test")).toMapByApplicationId
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("application id 'test/app' can contain only alphanumeric characters and '-', '_'")
  }

  "topology with a application id with an asterisk" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      applications = Map("test*app" -> Topology.Application("service-test")).toMapByApplicationId
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("application id 'test*app' can contain only alphanumeric characters and '-', '_'")
  }

  "topology with a alias topic id with a dot" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      aliases = LocalAliases(topics = Map("test.topic1" -> FlexibleNodeId.Regex(""".*""")).toAliases)
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("topic alias id 'test.topic1' can contain only alphanumeric characters and '-', '_'")
  }

  "topology with a alias topic id with a slash" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      aliases = LocalAliases(topics = Map("test/topic1" -> FlexibleNodeId.Regex(""".*""")).toAliases)
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("topic alias id 'test/topic1' can contain only alphanumeric characters and '-', '_'")
  }

  "topology with a alias topic id with an asterisk" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      aliases = LocalAliases(topics = Map("test*topic1" -> FlexibleNodeId.Regex(""".*""")).toAliases)
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("topic alias id 'test*topic1' can contain only alphanumeric characters and '-', '_'")
  }

  "topology with a alias application id with a dot" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      aliases = LocalAliases(applications = Map("test.app" -> FlexibleNodeId.Regex(""".*""")).toAliases)
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("application alias id 'test.app' can contain only alphanumeric characters and '-', '_'")
  }

  "topology with a alias application id with a slash" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      aliases = LocalAliases(applications = Map("test/app" -> FlexibleNodeId.Regex(""".*""")).toAliases)
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("application alias id 'test/app' can contain only alphanumeric characters and '-', '_'")
  }

  "topology with a alias application id with an asterisk" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      aliases = LocalAliases(applications = Map("test*app" -> FlexibleNodeId.Regex(""".*""")).toAliases)
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("application alias id 'test*app' can contain only alphanumeric characters and '-', '_'")
  }

  "empty topology" should "be valid" in {
    val actualValidationResult = validateTopology(TopologyEntityId("test"), Topology(topology = TopologyId("test")))
    actualValidationResult should beValid
  }

  "a simple source-sink topology" should "be valid" in {
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topologySourceSink())
    actualValidationResult should beValid
  }

  "a kafka-streams topology" should "be valid" in {
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topologyKafkaStreams())
    actualValidationResult should beValid
  }

  "a kafka-streams topology with multiple internal applications" should "be valid" in {
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topologyKafkaStreamsWithMultipleApplications())
    actualValidationResult should beValid
  }

  "a connector topology" should "be valid" in {
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topologyConnector())
    actualValidationResult should beValid
  }

  "a connect-replicator topology" should "be valid" in {
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topologyConnectReplicator())
    actualValidationResult should beValid
  }

  "a topology that has a topic with a name containing all alphanumeric and '.'/'-'/'_' characters" should "be valid" in {
    val topology = Topology(topology = TopologyId("test"), topics = Map("t" -> Topology.Topic("project.sub-project.-SomethingUpperCase.v1__kstream__")).toMapByTopicId)
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beValid
  }

  "a topology that has a topic with empty name" should "be invalid" in {
    val topology = Topology(topology = TopologyId("test"), topics = Map("t" -> Topology.Topic("")).toMapByTopicId)
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("topic name cannot be empty")
  }

  "a topology that has a topic with invalid name" should "be invalid" in {
    val topology = Topology(topology = TopologyId("test"), topics = Map("t" -> Topology.Topic("/kafka-topic")).toMapByTopicId)
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("contains invalid characters")
  }

  "a topology that has a topic with a too long name" should "be invalid" in {
    val topology = Topology(topology = TopologyId("test"), topics = Map("t" -> Topology.Topic("0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789")).toMapByTopicId)
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("is too long")
  }

  "a topology with topic duplication" should "be invalid" in {
    val topology = Topology(Namespace("test"), topics = Map("topic1" -> Topology.Topic("test.topic1"), "topic2" -> Topology.Topic("test.topic1")).toMapByTopicId)
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("duplicate topic names")
  }

  "a topology with topic id duplication including the aliases" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map("topic1" -> Topology.Topic("test.topic1")).toMapByTopicId,
      aliases = LocalAliases(topics = Map("topic1" -> FlexibleNodeId.Regex(""".*""")).toAliases)
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("duplicate topic ids: 'test.topic1'")
  }

  "a topology with application id duplication including the aliases" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      applications = Map("source" -> Topology.Application("service-test")).toMapByApplicationId,
      aliases = LocalAliases(applications = Map("source" -> FlexibleNodeId.Regex(""".*""")).toAliases)
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("duplicate application ids: 'test.source'")
  }

  "a topology with topic and application id duplication" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map("topic1" -> Topology.Topic("test.topic1")).toMapByTopicId,
      applications = Map("topic1" -> Topology.Application("service-test")).toMapByApplicationId,
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("duplicate ids: 'test.topic1'")
  }

  "a topology with application and topic alias id duplication" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      aliases = LocalAliases(
        topics = Map("dupe-id" -> FlexibleNodeId.Regex(""".*""")).toAliases,
        applications = Map("dupe-id" -> FlexibleNodeId.Regex(""".*""")).toAliases
      )
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("duplicate ids: 'test.dupe-id'")
  }

  "a topology with actual consumer group duplication" should "be valid" in {
    val topology = Topology(
      Namespace("test"),
      applications = Map(
        "app1" -> Topology.Application("service-test").makeSimple(Some("test.group")),
        "app2" -> Topology.Application("service-test").makeSimple(Some("test.group"))
      ).toMapByApplicationId
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beValid
    // for now it's valid
//        actualValidationResult should beInvalid
//        actualValidationResult should containMessage("duplicate consumer groups")
  }

  "a topology with transactional id duplication" should "be valid" in {
    val topology = Topology(
      Namespace("test"),
      applications = Map(
        "app1" -> Topology.Application("service-test").makeSimple(transactionalId = Some("test.trans-id")),
        "app2" -> Topology.Application("service-test").makeSimple(transactionalId = Some("test.trans-id"))
      ).toMapByApplicationId
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beValid
    // for now it's valid
//        actualValidationResult should beInvalid
//        actualValidationResult should containMessage("duplicate transactional ids")
  }

  "a topology with consumer group and kafka-streams application-id duplication" should "be valid" in {
    val topology = Topology(
      Namespace("test"),
      applications = Map(
        "app1" -> Topology.Application("service-test").makeSimple(Some("test.group")),
        "app2" -> Topology.Application("service-test").makeKafkaStreams("test.group")
      ).toMapByApplicationId
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beValid
    // for now it's valid
//        actualValidationResult should beInvalid
//        actualValidationResult should containMessage("duplicate consumer groups")
  }

  "a topology with kafka-streams application-id duplication" should "be valid" in {
    val topology = Topology(
      Namespace("test"),
      applications = Map(
        "app1" -> Topology.Application("service-test").makeKafkaStreams("test.group"),
        "app2" -> Topology.Application("service-test").makeKafkaStreams("test.group")
      ).toMapByApplicationId
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beValid
    // for now it's valid
//        actualValidationResult should beInvalid
//        actualValidationResult should containMessage("duplicate consumer groups")
  }

  "a topology that has a topic that's not part of its namespace" should "be invalid" in {
    val topology = Topology(Namespace("namespace"), topics = Map("topic1" -> Topology.Topic("test.topic1")).toMapByTopicId)
    val actualValidationResult = validateTopology(TopologyEntityId("namespace"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("topic names 'test.topic1' are not part of namespace 'namespace'")
  }

  "a topology that has an un-managed topic that's not part of its namespace" should "be invalid" in {
    val topology = Topology(Namespace("namespace"), topics = Map("topic1" -> Topology.Topic("test.topic1", unManaged = true)).toMapByTopicId)
    val actualValidationResult = validateTopology(TopologyEntityId("namespace"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("topic names 'test.topic1' are not part of namespace 'namespace'")
  }

  "a topology that has an application whose consumer group is not part of its namespace" should "be invalid" in {
    val topology = Topology(Namespace("namespace"), applications = Map("source" -> Topology.Application("service-test").makeSimple(Some("test.source"))).toMapByApplicationId)
    val actualValidationResult = validateTopology(TopologyEntityId("namespace"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("consumer groups 'test.source' are not part of namespace 'namespace'")
  }

  "a topology that has an application whose transactional id not part of its namespace" should "be invalid" in {
    val topology = Topology(Namespace("namespace"), applications = Map("source" -> Topology.Application("service-test").makeSimple(transactionalId = Some("test.source"))).toMapByApplicationId)
    val actualValidationResult = validateTopology(TopologyEntityId("namespace"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("transactional ids 'test.source' are not part of namespace 'namespace'")
  }

  "a topology that has an application and consumer group that's not part of its namespace" should "be invalid" in {
    val topology = Topology(Namespace("namespace"), applications = Map("source" -> Topology.Application("service-test").makeSimple(Some("test.source"))).toMapByApplicationId)
    val actualValidationResult = validateTopology(TopologyEntityId("namespace"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessages("consumer groups 'test.source' are not part of namespace 'namespace'")
  }

  "a topology that has a kafka streams application that's not part of its namespace" should "be invalid" in {
    val topology = Topology(Namespace("namespace"), applications = Map("source" -> Topology.Application("service-test").makeKafkaStreams("test.__kstreams__.Processor")).toMapByApplicationId)
    val actualValidationResult = validateTopology(TopologyEntityId("namespace"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("kafka streams application ids 'test.__kstreams__.Processor' are not part of namespace 'namespace'")
  }

  "a topology that has a kafka streams application and application that's not part of its namespace" should "be invalid" in {
    val topology = Topology(Namespace("namespace"), applications = Map("source" -> Topology.Application("service-test").makeKafkaStreams("test.__kstreams__.Processor")).toMapByApplicationId)
    val actualValidationResult = validateTopology(TopologyEntityId("namespace"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessages("kafka streams application ids 'test.__kstreams__.Processor' are not part of namespace 'namespace'")
  }

  "a topology that has a connector application and other consumable and producable namespaces" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      applications = Map(
        "source-connector" -> Topology.Application(
          "service-test",
          otherConsumableNamespaces = Seq(FlexibleName.Any()),
          otherProducableNamespaces = Seq(FlexibleName.Any())
        ).makeConnector("test-connector")
      ).toMapByApplicationId
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessages("connect application cannot have other consumable namespaces", "connect application cannot have other producable namespaces")
  }

  "a topology that has a connect-replicator application and other consumable and producable namespaces" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      applications = Map(
        "source-connector" -> Topology.Application(
          "service-test",
          otherConsumableNamespaces = Seq(FlexibleName.Any()),
          otherProducableNamespaces = Seq(FlexibleName.Any())
        ).makeConnectReplicator("test-connect-replicator")
      ).toMapByApplicationId
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessages("connect replicator application cannot have other consumable namespaces", "connect replicator application cannot have other producable namespaces")
  }

  "a topology that has an producing application without consumer group" should "be valid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map("topic1" -> Topology.Topic("test.topic1")).toMapByTopicId,
      applications = Map("source" -> Topology.Application("service-test")).toMapByApplicationId,
      relationships = Map(
        relationshipFrom("source", (RelationshipType.Produce(), Seq(("topic1", None))))
      )
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beValid
  }

  "a topology that has an consuming application without consumer group" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map("topic1" -> Topology.Topic("test.topic1")).toMapByTopicId,
      applications = Map("sink" -> Topology.Application("service-test")).toMapByApplicationId,
      relationships = Map(
        relationshipFrom("sink", (RelationshipType.Consume(), Seq(("topic1", None))))
      )
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("does not have a consumer group")
  }

  "a topology that has shared consuming application without consumer group" should "be invalid" in {
    val topology = Topology(
      Namespace("test"),
      topics = Map("topic1" -> Topology.Topic("test.topic1")).toMapByTopicId,
      applications = Map("sink" -> Topology.Application("service-test", otherConsumableNamespaces = Seq(FlexibleName.Any()))).toMapByApplicationId
    )
    val actualValidationResult = validateTopology(TopologyEntityId("test"), topology)
    actualValidationResult should beInvalid
    actualValidationResult should containMessage("simple application without a consumer group cannot have other consumable namespaces")
  }
}
