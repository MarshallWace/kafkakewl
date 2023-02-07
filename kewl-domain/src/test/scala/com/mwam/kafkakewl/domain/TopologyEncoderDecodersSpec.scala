/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import cats.syntax.option._
import org.scalatest.FlatSpec
import io.circe.syntax._
import io.circe.parser._
import com.mwam.kafkakewl.domain.JsonEncodeDecodeBase._
import com.mwam.kafkakewl.domain.topology.JsonEncodeDecode._
import com.mwam.kafkakewl.domain.topology._

class TopologyEncoderDecodersSpec extends FlatSpec
  with TestTopologiesCommon {

  /**
    * Topology
    */

  "deployment environment variables" should "be encoded/decoded correctly" in {
    def value(v: String*) = DeploymentEnvironment.Variable.Value(v)
    val environments = DeploymentEnvironments.Variables(
      DeploymentEnvironmentId("default") -> Map("partitions" -> value("100"), "empty" -> value()),
      DeploymentEnvironmentId("test-cluster") -> Map("partitions" -> value("4"), "service-user" -> value("service-kewltest-test"), "someList" -> value("1", "2")),
      DeploymentEnvironmentId("prod-cluster") -> Map("service-user" -> value("service-kewltest-prod"), "retention.ms" -> value("100000000000"), "someList" -> value("1", "2", "3"))
    )
    val environmentsAsJson = """{"default":{"partitions":"100","empty":null},"test-cluster":{"partitions":"4","service-user":"service-kewltest-test","someList":["1","2"]},"prod-cluster":{"service-user":"service-kewltest-prod","retention.ms":"100000000000","someList":["1","2","3"]}}"""
    assert(environments.asJson.noSpaces == environmentsAsJson)
    assert(environments == decode[DeploymentEnvironments.Variables](environmentsAsJson).right.get)

    // other ways to decode into the same
    assert(environments == decode[DeploymentEnvironments.Variables]("""{"default":{"partitions":["100"],"empty":[]},"test-cluster":{"partitions":"4","service-user":["service-kewltest-test"],"someList":["1","2"]},"prod-cluster":{"service-user":"service-kewltest-prod","retention.ms":"100000000000","someList":["1","2","3"]}}""").right.get)
  }

  /**
    * Topics
    */

  "empty topics" should "be encoded/decoded correctly" in {
    val topics = Map.empty[TopicId, Topology.Topic]
    val topicsAsJson = """{}"""
    assert(topics.asJson.noSpaces == topicsAsJson)
    assert(topics == decode[Map[TopicId, Topology.Topic]](topicsAsJson).right.get)
  }

  "topics with the same ids as names and with no expressions" should "be encoded correctly" in {
    val topics = Map(LocalTopicId("topic1") --> Topology.Topic("topic1"), LocalTopicId("topic2") --> Topology.Topic("topic2"))
    val topicsAsJson = """{"topic1":{"name":"topic1","partitions":1,"config":{}},"topic2":{"name":"topic2","partitions":1,"config":{}}}"""
    assert(topics.asJson.noSpaces == topicsAsJson)
    assert(topics == decode[Map[LocalTopicId, Topology.Topic]](topicsAsJson).right.get)

    // other ways to decode into the same topics
    assert(topics == decode[Map[LocalTopicId, Topology.Topic]]("""{"topic1":{"name":"topic1"},"topic2":{"name":"topic2"}}""").right.get)
    assert(topics == decode[Map[LocalTopicId, Topology.Topic]]("""{"topic1":{"name":"topic1","partitions":1},"topic2":{"name":"topic2","partitions":1}}""").right.get)
    assert(topics == decode[Map[LocalTopicId, Topology.Topic]]("""{"topic1":{"name":"topic1","partitions":1,"description":null,"otherConsumerNamespaces":[],"otherProducerNamespaces":[]},"topic2":{"name":"topic2","partitions":1}}""").right.get)
  }

  "topics with no expressions" should "be encoded correctly" in {
    val topics = Map(LocalTopicId("topic1Id") --> Topology.Topic("topic1"), LocalTopicId("topic2Id") --> Topology.Topic("topic2"))
    val topicsAsJson = """{"topic1Id":{"name":"topic1","partitions":1,"config":{}},"topic2Id":{"name":"topic2","partitions":1,"config":{}}}"""
    assert(topics.asJson.noSpaces == topicsAsJson)
    assert(topics == decode[Map[LocalTopicId, Topology.Topic]](topicsAsJson).right.get)

    // other ways to decode into the same topics
    assert(topics == decode[Map[LocalTopicId, Topology.Topic]]("""{"topic1Id":{"name":"topic1"},"topic2Id":{"name":"topic2"}}""").right.get)
    assert(topics == decode[Map[LocalTopicId, Topology.Topic]]("""{"topic1Id":{"name":"topic1","partitions":1},"topic2Id":{"name":"topic2"}}""").right.get)
    assert(topics == decode[Map[LocalTopicId, Topology.Topic]]("""{"topic1Id":{"name":"topic1","partitions":"1"},"topic2Id":{"name":"topic2"}}""").right.get)
    assert(topics == decode[Map[LocalTopicId, Topology.Topic]]("""{"topic1Id":{"name":"topic1","partitions":"1"},"topic2Id":{"name":"topic2","description":null,"otherConsumerNamespaces":[],"otherProducerNamespaces":[]}}""").right.get)
  }

  "topics with tags and labels" should "be encoded correctly" in {
    val topics = Map(LocalTopicId("topic1Id") --> Topology.Topic("topic1", tags = Seq(Topology.TagExpr("tag1")), labels = Map(Topology.LabelKeyExpr("label1") -> Topology.LabelValueExpr("value1"))))
    val topicsAsJson = """{"topic1Id":{"name":"topic1","partitions":1,"config":{},"tags":["tag1"],"labels":{"label1":"value1"}}}"""
    assert(topics.asJson.noSpaces == topicsAsJson)
    assert(topics == decode[Map[LocalTopicId, Topology.Topic]](topicsAsJson).right.get)

    // other ways to decode into the same topics
    assert(topics == decode[Map[LocalTopicId, Topology.Topic]]("""{"topic1Id":{"name":"topic1","tags":["tag1"],"labels":{"label1":"value1"}}}""").right.get)
    assert(topics == decode[Map[LocalTopicId, Topology.Topic]]("""{"topic1Id":{"name":"topic1","partitions":1,"tags":["tag1"],"labels":{"label1":"value1"}}}""").right.get)
    assert(topics == decode[Map[LocalTopicId, Topology.Topic]]("""{"topic1Id":{"name":"topic1","partitions":"1","tags":["tag1"],"labels":{"label1":"value1"}}}""").right.get)
    assert(topics == decode[Map[LocalTopicId, Topology.Topic]]("""{"topic1Id":{"name":"topic1","partitions":"1","description":null,"otherConsumerNamespaces":[],"otherProducerNamespaces":[],"tags":["tag1"],"labels":{"label1":"value1"}}}""").right.get)
  }

  "topics with some expressions" should "be encoded correctly" in {
    val topics = Map(LocalTopicId("topic1Id") --> Topology.Topic("topic1", partitions = Topology.PartitionsExpr("${partitions}")), LocalTopicId("topic2Id") --> Topology.Topic("topic2", partitions = Topology.PartitionsExpr("${partitions}")))
    val topicsAsJson = """{"topic1Id":{"name":"topic1","partitions":"${partitions}","config":{}},"topic2Id":{"name":"topic2","partitions":"${partitions}","config":{}}}"""
    assert(topics.asJson.noSpaces == topicsAsJson)
    assert(topics == decode[Map[LocalTopicId, Topology.Topic]](topicsAsJson).right.get)

    // other ways to decode into the same topics
    assert(topics == decode[Map[LocalTopicId, Topology.Topic]]("""{"topic1Id":{"name":"topic1","partitions":"${partitions}"},"topic2Id":{"name":"topic2","partitions":"${partitions}"}}""").right.get)
  }

  "topics with config expressions" should "be encoded correctly" in {
    val topics = Map(LocalTopicId("topic") --> Topology.Topic("topic", config = Map("retention.ms" -> Topology.TopicConfigExpr("${retention.ms}"))))
    val topicsAsJson = """{"topic":{"name":"topic","partitions":1,"config":{"retention.ms":"${retention.ms}"}}}"""
    assert(topics.asJson.noSpaces == topicsAsJson)
    assert(topics == decode[Map[LocalTopicId, Topology.Topic]](topicsAsJson).right.get)
  }

  "topics with replicationFactor" should "be encoded correctly" in {
    val topics = Map(LocalTopicId("topic") --> Topology.Topic("topic", replicationFactor = Topology.ReplicationFactorExpr("3").some))
    val topicsAsJson = """{"topic":{"name":"topic","partitions":1,"replicationFactor":3,"config":{}}}"""
    assert(topics.asJson.noSpaces == topicsAsJson)
    assert(topics == decode[Map[LocalTopicId, Topology.Topic]](topicsAsJson).right.get)
  }

  /**
    * Applications
    */

  "empty applications" should "be encoded/decoded correctly" in {
    val applications = Map.empty[LocalApplicationId, Topology.Application]
    val applicationsAsJson = """{}"""
    assert(applications.asJson.noSpaces == applicationsAsJson)
    assert(applications == decode[Map[LocalApplicationId, Topology.Application]](applicationsAsJson).right.get)
  }

  "various applications" should "be encoded/decoded correctly" in {
    val applications = Map(
      LocalApplicationId("simple-producer-app-id") --> Topology.Application(user = Topology.UserExpr("service-user"), tags = Seq(Topology.TagExpr("tag1")), labels = Map(Topology.LabelKeyExpr("label1") -> Topology.LabelValueExpr("value1"))),
      LocalApplicationId("simple-consumer-app-id") --> Topology.Application(user = Topology.UserExpr("service-user")).makeSimple(consumerGroup = Some("simple-consumer-app")),
      LocalApplicationId("kafkastreams-app-id") --> Topology.Application(user = Topology.UserExpr("${service-user}")).makeKafkaStreams(kafkaStreamsAppId = "kafkastreams-app"),
      LocalApplicationId("connector-app-id") --> Topology.Application(user = Topology.UserExpr("${service-user}")).makeConnector(connector = "connector-app"),
      LocalApplicationId("connect-replicator-app-id") --> Topology.Application(user = Topology.UserExpr("${service-user}")).makeConnectReplicator(connectReplicator = "connect-replicator")
    )
    val applicationsAsJson = """{"connector-app-id":{"user":"${service-user}","type":{"connector":"connector-app"},"consumerLagWindowSeconds":null},"kafkastreams-app-id":{"user":"${service-user}","type":{"kafkaStreamsAppId":"kafkastreams-app"},"consumerLagWindowSeconds":null},"connect-replicator-app-id":{"user":"${service-user}","type":{"connectReplicator":"connect-replicator"},"consumerLagWindowSeconds":null},"simple-consumer-app-id":{"user":"service-user","type":{"consumerGroup":"simple-consumer-app","transactionalId":null},"consumerLagWindowSeconds":null},"simple-producer-app-id":{"user":"service-user","type":{"consumerGroup":null,"transactionalId":null},"consumerLagWindowSeconds":null,"tags":["tag1"],"labels":{"label1":"value1"}}}"""
    assert(applications.asJson.noSpaces == applicationsAsJson)
    assert(applications == decode[Map[LocalApplicationId, Topology.Application]](applicationsAsJson).right.get)

    // other ways to decode into the same applications
    assert(applications == decode[Map[LocalApplicationId, Topology.Application]]("""{"connector-app-id":{"user":"${service-user}","type":{"connector":"connector-app"},"description":null,"otherConsumableNamespaces":[],"otherProducableNamespaces":[]},"kafkastreams-app-id":{"user":"${service-user}","type":{"kafkaStreamsAppId":"kafkastreams-app"}},"connect-replicator-app-id":{"user":"${service-user}","host":null,"type":{"connectReplicator":"connect-replicator"}},"simple-consumer-app-id":{"user":"service-user","type":{"consumerGroup":"simple-consumer-app"}},"simple-producer-app-id":{"user":"service-user","tags":["tag1"],"labels":{"label1":"value1"}}}""").right.get)
  }

  /**
    * Relationships
    */

  "default NodeRelationshipProperties" should "be encoded/decoded correctly" in {
    val nodeRelationship = Topology.NodeRelationshipProperties(NodeRef("NodeRef"))
    val nodeRelationshipAsJson = """{"NodeRef":{}}"""
    assert(nodeRelationship.asJson.noSpaces == nodeRelationshipAsJson)
    assert(nodeRelationship == decode[Topology.NodeRelationshipProperties](nodeRelationshipAsJson).right.get)

    // other ways to decode into the same node relationship
    assert(nodeRelationship == decode[Topology.NodeRelationshipProperties]("""{"NodeRef":{"reset":null}}""").right.get)
    assert(nodeRelationship == decode[Topology.NodeRelationshipProperties](""""NodeRef"""").right.get)
  }

  "NodeRelationshipProperties with a custom reset mode" should "be encoded/decoded correctly" in {
    val nodeRelationship = Topology.NodeRelationshipProperties(NodeRef("NodeRef"), Topology.RelationshipProperties(Some(ResetMode.End), tags = Seq(Topology.TagExpr("tag1")), labels = Map(Topology.LabelKeyExpr("label1") -> Topology.LabelValueExpr("value1"))))
    val nodeRelationshipAsJson = """{"NodeRef":{"reset":"End","tags":["tag1"],"labels":{"label1":"value1"}}}"""
    assert(nodeRelationship.asJson.noSpaces == nodeRelationshipAsJson)
    assert(nodeRelationship == decode[Topology.NodeRelationshipProperties](nodeRelationshipAsJson).right.get)

    // other ways to decode into the same node relationship
    assert(nodeRelationship != decode[Topology.NodeRelationshipProperties]("""{"NodeRef":{}}""").right.get)
    assert(nodeRelationship == decode[Topology.NodeRelationshipProperties]("""{"NodeRef":{"reset":"End","tags":["tag1"],"labels":{"label1":"value1"}}}""").right.get)
    assert(nodeRelationship != decode[Topology.NodeRelationshipProperties](""""NodeRef"""").right.get)
  }

  "NodeRelationshipProperties with a custom monitorConsumerLag flag" should "be encoded/decoded correctly" in {
    val nodeRelationship = Topology.NodeRelationshipProperties(NodeRef("NodeRef"), Topology.RelationshipProperties(None, Some(false), tags = Seq(Topology.TagExpr("tag1")), labels = Map(Topology.LabelKeyExpr("label1") -> Topology.LabelValueExpr("value1"))))
    val nodeRelationshipAsJson = """{"NodeRef":{"monitorConsumerLag":false,"tags":["tag1"],"labels":{"label1":"value1"}}}"""
    assert(nodeRelationship.asJson.noSpaces == nodeRelationshipAsJson)
    assert(nodeRelationship == decode[Topology.NodeRelationshipProperties](nodeRelationshipAsJson).right.get)

    // other ways to decode into the same node relationship
    assert(nodeRelationship == decode[Topology.NodeRelationshipProperties]("""{"NodeRef":{"monitorConsumerLag":false,"tags":["tag1"],"labels":{"label1":"value1"}}}""").right.get)
  }

  "NodeRelationships" should "be encoded/decoded correctly" in {
    val nodeRelationships: Topology.NodeRelationships = Map(RelationshipType.Produce() -> Seq(Topology.NodeRelationshipProperties(NodeRef("NodeRef1")), Topology.NodeRelationshipProperties(NodeRef("NodeRef2"))))
    val nodeRelationshipsAsJson = """{"produce":[{"NodeRef1":{}},{"NodeRef2":{}}]}"""
    assert(nodeRelationships.asJson.noSpaces == nodeRelationshipsAsJson)
    assert(nodeRelationships == decode[Topology.NodeRelationships](nodeRelationshipsAsJson).right.get)

    // other ways to decode into the same node relationships
    assert(nodeRelationships == decode[Topology.NodeRelationships]("""{"produce":["NodeRef1","NodeRef2"]}""").right.get)
    assert(decode[Topology.NodeRelationships]("""{"produce":[{"NodeRef1":{},"NodeRef2":{}}]}""").isLeft)
  }

  /**
    * Application types
    */

  "non-default kafka-streams application type" should "be encoded correctly" in {
    val kafkaStreamsType: Topology.Application.Type = Topology.Application.Type.KafkaStreams("kstreams-app-id")
    val kafkaStreamsTypeAsJson = """{"kafkaStreamsAppId":"kstreams-app-id"}"""
    assert(kafkaStreamsType.asJson.noSpaces == kafkaStreamsTypeAsJson)
  }

  "kafkaStreamsAppId=something" should "be decoded to a kafka-streams application type" in {
    val kafkaStreamsType: Topology.Application.Type = Topology.Application.Type.KafkaStreams("kstreams-app-id")
    val kafkaStreamsTypeAsJson = """{"kafkaStreamsAppId":"kstreams-app-id"}"""
    assert(kafkaStreamsType == decode[Topology.Application.Type](kafkaStreamsTypeAsJson).right.get)
  }
}
