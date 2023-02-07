/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import cats.syntax.option._
import com.mwam.kafkakewl.domain.topology.JsonEncodeDecode._
import com.mwam.kafkakewl.domain.topology._
import io.circe.parser._
import io.circe.syntax._
import org.scalatest.FlatSpec

class TopologyToDeployEncoderDecodersSpec extends FlatSpec
  with TestTopologiesToDeployCommon {

  /**
    * Topologies
    */

  "DevelopersAccess" should "be parsed correctly" in {
    assert(DevelopersAccess.parse("full").contains(DevelopersAccess.Full))
    assert(DevelopersAccess.parse("Full").contains(DevelopersAccess.Full))
    assert(DevelopersAccess.parse("FULL").contains(DevelopersAccess.Full))

    assert(DevelopersAccess.parse("topicreadonly").contains(DevelopersAccess.TopicReadOnly))
    assert(DevelopersAccess.parse("TopicReadOnly").contains(DevelopersAccess.TopicReadOnly))
    assert(DevelopersAccess.parse("TOPICREADONLY").contains(DevelopersAccess.TopicReadOnly))
  }

  "DevelopersAccess" should "be encoded/decoded correctly" in {
    assert(Topology.DevelopersAccessExpr("Full").asJson.noSpaces == """"Full"""" )
    assert(Topology.DevelopersAccessExpr("Full") == decode[Topology.DevelopersAccessExpr](""""Full"""").right.get)

    assert(Topology.DevelopersAccessExpr("TopicReadOnly").asJson.noSpaces == """"TopicReadOnly"""" )
    assert(Topology.DevelopersAccessExpr("TopicReadOnly") == decode[Topology.DevelopersAccessExpr](""""TopicReadOnly"""" ).right.get)
  }

  /**
    * Topics
    */

  "empty topics" should "be encoded/decoded correctly" in {
    val topics = Map.empty[TopicId, TopologyToDeploy.Topic]
    val topicsAsJson = """{}"""
    assert(topics.asJson.noSpaces == topicsAsJson)
    assert(topics == decode[Map[TopicId, TopologyToDeploy.Topic]](topicsAsJson).right.get)
  }

  "topics with the same ids as names" should "be encoded correctly" in {
    val topics = Map(LocalTopicId("topic1") --> TopologyToDeploy.Topic("topic1"), LocalTopicId("topic2") --> TopologyToDeploy.Topic("topic2"))
    val topicsAsJson = """{"topic1":{"name":"topic1","partitions":1,"config":{}},"topic2":{"name":"topic2","partitions":1,"config":{}}}"""
    assert(topics.asJson.noSpaces == topicsAsJson)
    assert(topics == decode[Map[LocalTopicId, TopologyToDeploy.Topic]](topicsAsJson).right.get)

    // other ways to decode into the same topics
    assert(topics == decode[Map[LocalTopicId, TopologyToDeploy.Topic]]("""{"topic1":{"name":"topic1"},"topic2":{"name":"topic2"}}""").right.get)
    assert(topics == decode[Map[LocalTopicId, TopologyToDeploy.Topic]]("""{"topic1":{"name":"topic1","partitions":1},"topic2":{"name":"topic2","partitions":1}}""").right.get)
    assert(topics == decode[Map[LocalTopicId, TopologyToDeploy.Topic]]("""{"topic1":{"name":"topic1","partitions":1,"description":null,"otherConsumerNamespaces":[],"otherProducerNamespaces":[]},"topic2":{"name":"topic2","partitions":1}}""").right.get)
  }

  "topics" should "be encoded correctly" in {
    val topics = Map(LocalTopicId("topic1Id") --> TopologyToDeploy.Topic("topic1"), LocalTopicId("topic2Id") --> TopologyToDeploy.Topic("topic2"))
    val topicsAsJson = """{"topic1Id":{"name":"topic1","partitions":1,"config":{}},"topic2Id":{"name":"topic2","partitions":1,"config":{}}}"""
    assert(topics.asJson.noSpaces == topicsAsJson)
    assert(topics == decode[Map[LocalTopicId, TopologyToDeploy.Topic]](topicsAsJson).right.get)

    // other ways to decode into the same topics
    assert(topics == decode[Map[LocalTopicId, TopologyToDeploy.Topic]]("""{"topic1Id":{"name":"topic1"},"topic2Id":{"name":"topic2"}}""").right.get)
    assert(topics == decode[Map[LocalTopicId, TopologyToDeploy.Topic]]("""{"topic1Id":{"name":"topic1","partitions":1},"topic2Id":{"name":"topic2"}}""").right.get)
    assert(topics == decode[Map[LocalTopicId, TopologyToDeploy.Topic]]("""{"topic1Id":{"name":"topic1","partitions":"1"},"topic2Id":{"name":"topic2"}}""").right.get)
    assert(topics == decode[Map[LocalTopicId, TopologyToDeploy.Topic]]("""{"topic1Id":{"name":"topic1","partitions":"1"},"topic2Id":{"name":"topic2","description":null,"otherConsumerNamespaces":[],"otherProducerNamespaces":[]}}""").right.get)
  }

  "topics with tags and labels" should "be encoded correctly" in {
    val topics = Map(LocalTopicId("topic1Id") --> TopologyToDeploy.Topic("topic1", tags = Seq("tag1"), labels = Map("label1" -> "value1")))
    val topicsAsJson = """{"topic1Id":{"name":"topic1","partitions":1,"config":{},"tags":["tag1"],"labels":{"label1":"value1"}}}"""
    assert(topics.asJson.noSpaces == topicsAsJson)
    assert(topics == decode[Map[LocalTopicId, TopologyToDeploy.Topic]](topicsAsJson).right.get)

    // other ways to decode into the same topics
    assert(topics == decode[Map[LocalTopicId, TopologyToDeploy.Topic]]("""{"topic1Id":{"name":"topic1","tags":["tag1"],"labels":{"label1":"value1"}}}""").right.get)
    assert(topics == decode[Map[LocalTopicId, TopologyToDeploy.Topic]]("""{"topic1Id":{"name":"topic1","partitions":1,"tags":["tag1"],"labels":{"label1":"value1"}}}""").right.get)
    assert(topics == decode[Map[LocalTopicId, TopologyToDeploy.Topic]]("""{"topic1Id":{"name":"topic1","partitions":"1","tags":["tag1"],"labels":{"label1":"value1"}}}""").right.get)
    assert(topics == decode[Map[LocalTopicId, TopologyToDeploy.Topic]]("""{"topic1Id":{"name":"topic1","partitions":"1","description":null,"otherConsumerNamespaces":[],"otherProducerNamespaces":[],"tags":["tag1"],"labels":{"label1":"value1"}}}""").right.get)
  }

  "topics with replicationFactor" should "be encoded correctly" in {
    val topics = Map(LocalTopicId("topic1Id") --> TopologyToDeploy.Topic("topic1", replicationFactor = 3.toShort.some))
    val topicsAsJson = """{"topic1Id":{"name":"topic1","partitions":1,"replicationFactor":3,"config":{}}}"""
    assert(topics.asJson.noSpaces == topicsAsJson)
    assert(topics == decode[Map[LocalTopicId, TopologyToDeploy.Topic]](topicsAsJson).right.get)

    // other ways to decode into the same topics
    assert(topics == decode[Map[LocalTopicId, TopologyToDeploy.Topic]]("""{"topic1Id":{"name":"topic1","replicationFactor":3}}""").right.get)
    assert(topics == decode[Map[LocalTopicId, TopologyToDeploy.Topic]]("""{"topic1Id":{"name":"topic1","partitions":1,"replicationFactor":3}}""").right.get)
    assert(topics == decode[Map[LocalTopicId, TopologyToDeploy.Topic]]("""{"topic1Id":{"name":"topic1","partitions":"1","replicationFactor":"3"}}""").right.get)
    assert(topics == decode[Map[LocalTopicId, TopologyToDeploy.Topic]]("""{"topic1Id":{"name":"topic1","partitions":"1","replicationFactor":"3","description":null,"otherConsumerNamespaces":[],"otherProducerNamespaces":[]}}""").right.get)
  }

  /**
    * Applications
    */

  "empty applications" should "be encoded/decoded correctly" in {
    val applications = Map.empty[ApplicationId, TopologyToDeploy.Application]
    val applicationsAsJson = """{}"""
    assert(applications.asJson.noSpaces == applicationsAsJson)
    assert(applications == decode[Map[ApplicationId, TopologyToDeploy.Application]](applicationsAsJson).right.get)
  }

  "various applications" should "be encoded/decoded correctly" in {
    val applications = Map(
      LocalApplicationId("simple-producer-app-id") --> TopologyToDeploy.Application(user = "service-user", tags = Seq("tag1"), labels = Map("label1" -> "value1")),
      LocalApplicationId("simple-consumer-app-id") --> TopologyToDeploy.Application(user = "service-user").makeSimple(consumerGroup = Some("simple-consumer-app")),
      LocalApplicationId("kafkastreams-app-id") --> TopologyToDeploy.Application(user = "${service-user}").makeKafkaStreams(kafkaStreamsAppId = "kafkastreams-app"),
      LocalApplicationId("connector-app-id") --> TopologyToDeploy.Application(user = "${service-user}").makeConnector(connector = "connector-app"),
      LocalApplicationId("connect-replicator-app-id") --> TopologyToDeploy.Application(user = "${service-user}").makeConnectReplicator(connectReplicator = "connect-replicator")
    )
    val applicationsAsJson = """{"connector-app-id":{"user":"${service-user}","type":{"connector":"connector-app"},"consumerLagWindowSeconds":null},"kafkastreams-app-id":{"user":"${service-user}","type":{"kafkaStreamsAppId":"kafkastreams-app"},"consumerLagWindowSeconds":null},"connect-replicator-app-id":{"user":"${service-user}","type":{"connectReplicator":"connect-replicator"},"consumerLagWindowSeconds":null},"simple-consumer-app-id":{"user":"service-user","type":{"consumerGroup":"simple-consumer-app","transactionalId":null},"consumerLagWindowSeconds":null},"simple-producer-app-id":{"user":"service-user","type":{"consumerGroup":null,"transactionalId":null},"consumerLagWindowSeconds":null,"tags":["tag1"],"labels":{"label1":"value1"}}}"""
    assert(applications.asJson.noSpaces == applicationsAsJson)
    assert(applications == decode[Map[LocalApplicationId, TopologyToDeploy.Application]](applicationsAsJson).right.get)

    // other ways to decode into the same applications
    assert(applications == decode[Map[LocalApplicationId, TopologyToDeploy.Application]]("""{"connector-app-id":{"user":"${service-user}","type":{"connector":"connector-app"}},"kafkastreams-app-id":{"user":"${service-user}","type":{"kafkaStreamsAppId":"kafkastreams-app"}},"connect-replicator-app-id":{"user":"${service-user}","type":{"connectReplicator":"connect-replicator"}},"simple-consumer-app-id":{"user":"service-user","type":{"consumerGroup":"simple-consumer-app"}},"simple-producer-app-id":{"user":"service-user","tags":["tag1"],"labels":{"label1":"value1"}}}""").right.get)
    assert(applications == decode[Map[LocalApplicationId, TopologyToDeploy.Application]]("""{"connector-app-id":{"user":"${service-user}","host":null,"type":{"connector":"connector-app"}},"kafkastreams-app-id":{"user":"${service-user}","type":{"kafkaStreamsAppId":"kafkastreams-app"}},"connect-replicator-app-id":{"user":"${service-user}","type":{"connectReplicator":"connect-replicator"},"description":null,"otherConsumableNamespaces":[],"otherProducableNamespaces":[]},"simple-consumer-app-id":{"user":"service-user","type":{"consumerGroup":"simple-consumer-app"}},"simple-producer-app-id":{"user":"service-user","tags":["tag1"],"labels":{"label1":"value1"}}}""").right.get)
  }

  /**
    * Relationships
    */

  "default NodeRelationshipProperties" should "be encoded/decoded correctly" in {
    val nodeRelationship = TopologyToDeploy.NodeRelationshipProperties(NodeRef("NodeRef"))
    val nodeRelationshipAsJson = """{"NodeRef":{}}"""
    assert(nodeRelationship.asJson.noSpaces == nodeRelationshipAsJson)
    assert(nodeRelationship == decode[TopologyToDeploy.NodeRelationshipProperties](nodeRelationshipAsJson).right.get)

    // other ways to decode into the same node relationship
    assert(nodeRelationship == decode[TopologyToDeploy.NodeRelationshipProperties]("""{"NodeRef":{"reset":null}}""").right.get)
    assert(nodeRelationship == decode[TopologyToDeploy.NodeRelationshipProperties](""""NodeRef"""").right.get)
  }

  "NodeRelationshipProperties with a custom reset mode" should "be encoded/decoded correctly" in {
    val nodeRelationship = TopologyToDeploy.NodeRelationshipProperties(NodeRef("NodeRef"), TopologyToDeploy.RelationshipProperties(Some(ResetMode.End), tags = Seq("tag1"), labels = Map("label1" -> "value1")))
    val nodeRelationshipAsJson = """{"NodeRef":{"reset":"End","tags":["tag1"],"labels":{"label1":"value1"}}}"""
    assert(nodeRelationship.asJson.noSpaces == nodeRelationshipAsJson)
    assert(nodeRelationship == decode[TopologyToDeploy.NodeRelationshipProperties](nodeRelationshipAsJson).right.get)

    // other ways to decode into the same node relationship
    assert(nodeRelationship != decode[TopologyToDeploy.NodeRelationshipProperties]("""{"NodeRef":{}}""").right.get)
    assert(nodeRelationship == decode[TopologyToDeploy.NodeRelationshipProperties]("""{"NodeRef":{"reset":"End","tags":["tag1"],"labels":{"label1":"value1"}}}""").right.get)
    assert(nodeRelationship != decode[TopologyToDeploy.NodeRelationshipProperties](""""NodeRef"""").right.get)
  }

  "NodeRelationshipProperties with a custom monitorConsumerLag flag" should "be encoded/decoded correctly" in {
    val nodeRelationship = TopologyToDeploy.NodeRelationshipProperties(NodeRef("NodeRef"), TopologyToDeploy.RelationshipProperties(None, Some(false), tags = Seq("tag1"), labels = Map("label1" -> "value1")))
    val nodeRelationshipAsJson = """{"NodeRef":{"monitorConsumerLag":false,"tags":["tag1"],"labels":{"label1":"value1"}}}"""
    assert(nodeRelationship.asJson.noSpaces == nodeRelationshipAsJson)
    assert(nodeRelationship == decode[TopologyToDeploy.NodeRelationshipProperties](nodeRelationshipAsJson).right.get)

    // other ways to decode into the same node relationship
    assert(nodeRelationship == decode[TopologyToDeploy.NodeRelationshipProperties]("""{"NodeRef":{"monitorConsumerLag":false,"tags":["tag1"],"labels":{"label1":"value1"}}}""").right.get)
  }

  "NodeRelationships" should "be encoded/decoded correctly" in {
    val nodeRelationships: TopologyToDeploy.NodeRelationships = Map(RelationshipType.Produce() -> Seq(TopologyToDeploy.NodeRelationshipProperties(NodeRef("NodeRef1")), TopologyToDeploy.NodeRelationshipProperties(NodeRef("NodeRef2"))))
    val nodeRelationshipsAsJson = """{"produce":[{"NodeRef1":{}},{"NodeRef2":{}}]}"""
    assert(nodeRelationships.asJson.noSpaces == nodeRelationshipsAsJson)
    assert(nodeRelationships == decode[TopologyToDeploy.NodeRelationships](nodeRelationshipsAsJson).right.get)

    // other ways to decode into the same node relationships
    assert(nodeRelationships == decode[TopologyToDeploy.NodeRelationships]("""{"produce":["NodeRef1","NodeRef2"]}""").right.get)
    assert(decode[Topology.NodeRelationships]("""{"produce":[{"NodeRef1":{},"NodeRef2":{}}]}""").isLeft)
  }

  /**
    * Application types
    */

  "non-default kafka-streams application type" should "be encoded correctly" in {
    val kafkaStreamsType: TopologyToDeploy.Application.Type = TopologyToDeploy.Application.Type.KafkaStreams("kstreams-app-id")
    val kafkaStreamsTypeAsJson = """{"kafkaStreamsAppId":"kstreams-app-id"}"""
    assert(kafkaStreamsType.asJson.noSpaces == kafkaStreamsTypeAsJson)
  }

  "kafkaStreamsAppId=something" should "be decoded to a kafka-streams application type" in {
    val kafkaStreamsType: TopologyToDeploy.Application.Type = TopologyToDeploy.Application.Type.KafkaStreams("kstreams-app-id")
    val kafkaStreamsTypeAsJson = """{"kafkaStreamsAppId":"kstreams-app-id"}"""
    assert(kafkaStreamsType == decode[TopologyToDeploy.Application.Type](kafkaStreamsTypeAsJson).right.get)
  }
}
