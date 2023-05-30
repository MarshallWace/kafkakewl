/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import cats.syntax.option._
import com.mwam.kafkakewl.domain.JsonEncodeDecodeBase._
import com.mwam.kafkakewl.domain.kafka.config.TopicConfigDefault
import com.mwam.kafkakewl.domain.kafkacluster.JsonEncodeDecode._
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId, TopicConfigKeyConstraint, TopicConfigKeyConstraintInclusive}
import com.mwam.kafkakewl.domain.topology.ReplicaPlacementId
import io.circe.parser.decode
import io.circe.syntax._
import org.scalatest.FlatSpec

class KafkaClusterEncoderDecodersSpec extends FlatSpec {
  val replicaPlacementJson = """{"durable":true}"""
  val defaultTopicConfig: (String, TopicConfigDefault) = TopicConfigDefault.fromReplicaPlacement(replicaPlacementJson)

  "list of environments in the normal order" should "be decoded with keeping the order in the json" in {
    val expected = DeploymentEnvironments.OrderedVariables(
      DeploymentEnvironmentId("default") -> DeploymentEnvironment.Variables.empty,
      DeploymentEnvironmentId("test") -> DeploymentEnvironment.Variables.empty,
      DeploymentEnvironmentId("test-cluster") -> DeploymentEnvironment.Variables.empty
    )
    assert(decode[DeploymentEnvironments.OrderedVariables]("""["default", "test", "test-cluster"]""") == Right(expected))
    assert(decode[DeploymentEnvironments.OrderedVariables]("""["test", "default", "test-cluster"]""") != Right(expected))
    assert(decode[DeploymentEnvironments.OrderedVariables]("""["test-cluster", "test", "default"]""") != Right(expected))
  }

  "list of environments in the opposite order" should "be decoded with keeping the order in the json" in {
    val expected = DeploymentEnvironments.OrderedVariables(
      DeploymentEnvironmentId("test-cluster") -> DeploymentEnvironment.Variables.empty,
      DeploymentEnvironmentId("test") -> DeploymentEnvironment.Variables.empty,
      DeploymentEnvironmentId("default") -> DeploymentEnvironment.Variables.empty
    )
    assert(decode[DeploymentEnvironments.OrderedVariables]("""["default", "test", "test-cluster"]""") != Right(expected))
    assert(decode[DeploymentEnvironments.OrderedVariables]("""["test", "default", "test-cluster"]""") != Right(expected))
    assert(decode[DeploymentEnvironments.OrderedVariables]("""["test-cluster", "test", "default"]""") == Right(expected))
  }

  "map of environments in the normal order" should "be decoded with keeping the order in the json" in {
    val expected = DeploymentEnvironments.OrderedVariables(
      DeploymentEnvironmentId("default") -> DeploymentEnvironment.Variables("key1" -> DeploymentEnvironment.Variable.Value(Seq("value1"))),
      DeploymentEnvironmentId("test") -> DeploymentEnvironment.Variables("key2" -> DeploymentEnvironment.Variable.Value(Seq("value2"))),
      DeploymentEnvironmentId("test-cluster") -> DeploymentEnvironment.Variables("key3" -> DeploymentEnvironment.Variable.Value(Seq("value3")))
    )
    assert(decode[DeploymentEnvironments.OrderedVariables]("""{"default":{"key1":"value1"}, "test":{"key2":"value2"}, "test-cluster":{"key3":"value3"}}""") == Right(expected))
    assert(decode[DeploymentEnvironments.OrderedVariables]("""{"test":{"key2":"value2"}, "default":{"key1":"value1"}, "test-cluster":{"key3":"value3"}}""") != Right(expected))
    assert(decode[DeploymentEnvironments.OrderedVariables]("""{"test-cluster":{"key3":"value3"}, "test":{"key2":"value2"}, "default":{"key1":"value1"}}""") != Right(expected))
  }

  "map of environments in the opposite order" should "be decoded with keeping the order in the json" in {
    val expected = DeploymentEnvironments.OrderedVariables(
      DeploymentEnvironmentId("test-cluster") -> DeploymentEnvironment.Variables("key3" -> DeploymentEnvironment.Variable.Value(Seq("value3"))),
      DeploymentEnvironmentId("test") -> DeploymentEnvironment.Variables("key2" -> DeploymentEnvironment.Variable.Value(Seq("value2"))),
      DeploymentEnvironmentId("default") -> DeploymentEnvironment.Variables("key1" -> DeploymentEnvironment.Variable.Value(Seq("value1"))),
    )
    assert(decode[DeploymentEnvironments.OrderedVariables]("""{"default":{"key1":"value1"}, "test":{"key2":"value2"}, "test-cluster":{"key3":"value3"}}""") != Right(expected))
    assert(decode[DeploymentEnvironments.OrderedVariables]("""{"test":{"key2":"value2"}, "default":{"key1":"value1"}, "test-cluster":{"key3":"value3"}}""") != Right(expected))
    assert(decode[DeploymentEnvironments.OrderedVariables]("""{"test-cluster":{"key3":"value3"}, "test":{"key2":"value2"}, "default":{"key1":"value1"}}""") == Right(expected))
  }

  "a KafkaCluster with no environments" should "be encoded/decoded correctly" in {
    val kafkaCluster = KafkaCluster(KafkaClusterEntityId("test"), "broker1,broker2,broker3")
    val kafkaClusterAsJson = """{"kafkaCluster":"test","brokers":"broker1,broker2,broker3","zooKeeper":null,"zooKeeperRetryIntervalMs":null,"securityProtocol":null,"kafkaClientConfig":{},"name":"","environments":{},"nonKewl":{"topicRegexes":[],"topicForAclsRegexes":[],"groupForAclsRegexes":[],"transactionalIdForAclsRegexes":[],"kafkaAcls":[]},"requiresAuthorizationCode":false,"replicaPlacementConfigs":{},"defaultReplicaPlacementId":null,"topicConfigKeysAllowedInTopologies":{"include":[],"exclude":[]},"additionalManagedTopicConfigKeys":{"include":[],"exclude":[]},"systemTopicsReplicaPlacementId":null,"kafkaRequestTimeOutMillis":null}"""
    assert(kafkaCluster.asJson.noSpaces == kafkaClusterAsJson)
    assert(kafkaCluster == decode[KafkaCluster](kafkaClusterAsJson).right.get)

    // other ways to decode into the same
    assert(kafkaCluster == decode[KafkaCluster]("""{"kafkaCluster":"test","brokers":"broker1,broker2,broker3","environments":{}}""").right.get)
    assert(kafkaCluster == decode[KafkaCluster]("""{"kafkaCluster":"test","brokers":"broker1,broker2,broker3","environments":[]}""").right.get)
  }

  "a KafkaCluster with zooKeeper" should "be encoded/decoded correctly" in {
    val kafkaCluster = KafkaCluster(KafkaClusterEntityId("test"), "broker1,broker2,broker3", "zookeeper1,zookeeper2,zookeeper3".some, 3000.some)
    val kafkaClusterAsJson = """{"kafkaCluster":"test","brokers":"broker1,broker2,broker3","zooKeeper":"zookeeper1,zookeeper2,zookeeper3","zooKeeperRetryIntervalMs":3000,"securityProtocol":null,"kafkaClientConfig":{},"name":"","environments":{},"nonKewl":{"topicRegexes":[],"topicForAclsRegexes":[],"groupForAclsRegexes":[],"transactionalIdForAclsRegexes":[],"kafkaAcls":[]},"requiresAuthorizationCode":false,"replicaPlacementConfigs":{},"defaultReplicaPlacementId":null,"topicConfigKeysAllowedInTopologies":{"include":[],"exclude":[]},"additionalManagedTopicConfigKeys":{"include":[],"exclude":[]},"systemTopicsReplicaPlacementId":null,"kafkaRequestTimeOutMillis":null}"""
    assert(kafkaCluster.asJson.noSpaces == kafkaClusterAsJson)
    assert(kafkaCluster == decode[KafkaCluster](kafkaClusterAsJson).right.get)
  }

  "a KafkaCluster with some environments but not variables" should "be encoded/decoded correctly" in {
    val kafkaCluster = KafkaCluster(
      KafkaClusterEntityId("test"),
      "broker1,broker2,broker3",
      environments = DeploymentEnvironments.OrderedVariables(
        DeploymentEnvironmentId("default") -> DeploymentEnvironment.Variables.empty,
        DeploymentEnvironmentId("test") -> DeploymentEnvironment.Variables.empty,
        DeploymentEnvironmentId("test-cluster") -> DeploymentEnvironment.Variables.empty
      )
    )
    val kafkaClusterAsJson = """{"kafkaCluster":"test","brokers":"broker1,broker2,broker3","zooKeeper":null,"zooKeeperRetryIntervalMs":null,"securityProtocol":null,"kafkaClientConfig":{},"name":"","environments":{"default":{},"test":{},"test-cluster":{}},"nonKewl":{"topicRegexes":[],"topicForAclsRegexes":[],"groupForAclsRegexes":[],"transactionalIdForAclsRegexes":[],"kafkaAcls":[]},"requiresAuthorizationCode":false,"replicaPlacementConfigs":{},"defaultReplicaPlacementId":null,"topicConfigKeysAllowedInTopologies":{"include":[],"exclude":[]},"additionalManagedTopicConfigKeys":{"include":[],"exclude":[]},"systemTopicsReplicaPlacementId":null,"kafkaRequestTimeOutMillis":null}"""
    assert(kafkaCluster.asJson.noSpaces == kafkaClusterAsJson)
    assert(kafkaCluster == decode[KafkaCluster](kafkaClusterAsJson).right.get)

    // other ways to decode into the same
    assert(kafkaCluster == decode[KafkaCluster]("""{"kafkaCluster":"test","brokers":"broker1,broker2,broker3","environments":{"default":{},"test":{},"test-cluster":{}}}""").right.get)
    assert(kafkaCluster == decode[KafkaCluster]("""{"kafkaCluster":"test","brokers":"broker1,broker2,broker3","environments":["default", "test", "test-cluster"]}""").right.get)
  }

  "a KafkaCluster with some environments variables" should "be encoded/decoded correctly" in {
    val kafkaCluster = KafkaCluster(
      KafkaClusterEntityId("test"),
      "broker1,broker2,broker3",
      environments = DeploymentEnvironments.OrderedVariables(
        DeploymentEnvironmentId("default") -> DeploymentEnvironment.Variables("key" -> DeploymentEnvironment.Variable.Value(Seq("val-default"))),
        DeploymentEnvironmentId("test") -> DeploymentEnvironment.Variables("key" -> DeploymentEnvironment.Variable.Value(Seq("val-test"))),
        DeploymentEnvironmentId("test-cluster") -> DeploymentEnvironment.Variables("key" -> DeploymentEnvironment.Variable.Value(Seq("val-test-cluster")))
      )
    )
    val kafkaClusterAsJson = """{"kafkaCluster":"test","brokers":"broker1,broker2,broker3","zooKeeper":null,"zooKeeperRetryIntervalMs":null,"securityProtocol":null,"kafkaClientConfig":{},"name":"","environments":{"default":{"key":"val-default"},"test":{"key":"val-test"},"test-cluster":{"key":"val-test-cluster"}},"nonKewl":{"topicRegexes":[],"topicForAclsRegexes":[],"groupForAclsRegexes":[],"transactionalIdForAclsRegexes":[],"kafkaAcls":[]},"requiresAuthorizationCode":false,"replicaPlacementConfigs":{},"defaultReplicaPlacementId":null,"topicConfigKeysAllowedInTopologies":{"include":[],"exclude":[]},"additionalManagedTopicConfigKeys":{"include":[],"exclude":[]},"systemTopicsReplicaPlacementId":null,"kafkaRequestTimeOutMillis":null}"""
    assert(kafkaCluster.asJson.noSpaces == kafkaClusterAsJson)
    assert(kafkaCluster == decode[KafkaCluster](kafkaClusterAsJson).right.get)

    // other ways to decode into the same
    assert(kafkaCluster == decode[KafkaCluster]("""{"kafkaCluster":"test","brokers":"broker1,broker2,broker3","environments":{"default":{"key":"val-default"},"test":{"key":"val-test"},"test-cluster":{"key":"val-test-cluster"}}}""").right.get)
  }

  "a KafkaCluster with replicaPlacements" should "be encoded/decoded correctly" in {
    val kafkaCluster = KafkaCluster(KafkaClusterEntityId("test"), "broker1,broker2,broker3", replicaPlacementConfigs = Map(ReplicaPlacementId("default") -> Map(defaultTopicConfig)), defaultReplicaPlacementId = ReplicaPlacementId("default").some)
    val kafkaClusterAsJson = """{"kafkaCluster":"test","brokers":"broker1,broker2,broker3","zooKeeper":null,"zooKeeperRetryIntervalMs":null,"securityProtocol":null,"kafkaClientConfig":{},"name":"","environments":{},"nonKewl":{"topicRegexes":[],"topicForAclsRegexes":[],"groupForAclsRegexes":[],"transactionalIdForAclsRegexes":[],"kafkaAcls":[]},"requiresAuthorizationCode":false,"replicaPlacementConfigs":{"default":{"confluent.placement.constraints":{"overridable":false,"default":"{\"durable\":true}"}}},"defaultReplicaPlacementId":"default","topicConfigKeysAllowedInTopologies":{"include":[],"exclude":[]},"additionalManagedTopicConfigKeys":{"include":[],"exclude":[]},"systemTopicsReplicaPlacementId":null,"kafkaRequestTimeOutMillis":null}"""
    assert(kafkaCluster.asJson.noSpaces == kafkaClusterAsJson)
    assert(kafkaCluster == decode[KafkaCluster](kafkaClusterAsJson).right.get)

    // other ways to decode into the same
    assert(kafkaCluster == decode[KafkaCluster]("""{"kafkaCluster":"test","brokers":"broker1,broker2,broker3","environments":{},"replicaPlacementConfigs":{"default":{"confluent.placement.constraints":{"overridable":false,"default":"{\"durable\":true}"}}},"defaultReplicaPlacementId":"default"}""").right.get)
    assert(kafkaCluster == decode[KafkaCluster]("""{"kafkaCluster":"test","brokers":"broker1,broker2,broker3","environments":[],"replicaPlacementConfigs":{"default":{"confluent.placement.constraints":{"overridable":false,"default":"{\"durable\":true}"}}},"defaultReplicaPlacementId":"default"}""").right.get)
  }

  "a KafkaCluster with allowed topic config keys" should "be encoded/decoded correctly" in {
    val kafkaCluster = KafkaCluster(
      KafkaClusterEntityId("test"),
      "broker1,broker2,broker3",
      replicaPlacementConfigs = Map(ReplicaPlacementId("default") -> Map(defaultTopicConfig)), defaultReplicaPlacementId = ReplicaPlacementId("default").some,
      topicConfigKeysAllowedInTopologies = TopicConfigKeyConstraintInclusive(Seq(TopicConfigKeyConstraint.Exact("a"), TopicConfigKeyConstraint.Regex("b"), TopicConfigKeyConstraint.Prefix("c")))
    )
    val kafkaClusterAsJson = """{"kafkaCluster":"test","brokers":"broker1,broker2,broker3","zooKeeper":null,"zooKeeperRetryIntervalMs":null,"securityProtocol":null,"kafkaClientConfig":{},"name":"","environments":{},"nonKewl":{"topicRegexes":[],"topicForAclsRegexes":[],"groupForAclsRegexes":[],"transactionalIdForAclsRegexes":[],"kafkaAcls":[]},"requiresAuthorizationCode":false,"replicaPlacementConfigs":{"default":{"confluent.placement.constraints":{"overridable":false,"default":"{\"durable\":true}"}}},"defaultReplicaPlacementId":"default","topicConfigKeysAllowedInTopologies":{"include":["a",{"regex":"b"},{"prefix":"c"}],"exclude":[]},"additionalManagedTopicConfigKeys":{"include":[],"exclude":[]},"systemTopicsReplicaPlacementId":null,"kafkaRequestTimeOutMillis":null}"""
    assert(kafkaCluster.asJson.noSpaces == kafkaClusterAsJson)
    assert(kafkaCluster == decode[KafkaCluster](kafkaClusterAsJson).right.get)

    // other ways to decode into the same
    assert(kafkaCluster == decode[KafkaCluster]("""{"kafkaCluster":"test","brokers":"broker1,broker2,broker3","environments":{},"replicaPlacementConfigs":{"default":{"confluent.placement.constraints":{"overridable":false,"default":"{\"durable\":true}"}}},"defaultReplicaPlacementId":"default","topicConfigKeysAllowedInTopologies":{"include":["a",{"regex":"b"},{"prefix":"c"}]}}""").right.get)
    assert(kafkaCluster == decode[KafkaCluster]("""{"kafkaCluster":"test","brokers":"broker1,broker2,broker3","environments":[],"replicaPlacementConfigs":{"default":{"confluent.placement.constraints":{"overridable":false,"default":"{\"durable\":true}"}}},"defaultReplicaPlacementId":"default","topicConfigKeysAllowedInTopologies":{"include":[{"exact":"a"},{"regex":"b"},{"prefix":"c"}],"exclude":[]}}""").right.get)
  }
}
