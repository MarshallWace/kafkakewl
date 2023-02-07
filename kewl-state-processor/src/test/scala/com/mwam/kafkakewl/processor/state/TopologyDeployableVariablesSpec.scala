/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.state

import com.mwam.kafkakewl.domain.Expressions.Variables
import com.mwam.kafkakewl.domain.{TestTopologies, TestTopologiesCommon}
import org.scalatest.{FlatSpec, Matchers}

class TopologyDeployableVariablesSpec extends FlatSpec
  with Matchers
  with MakeDeployableMatchers
  with TestTopologies
  with TestTopologiesCommon
  with TopologyDeployableCommon {

  "a topology with no environment variables" should "produce no additional variables" in {
    val topology = topologySourceSink()
    val actualVariables = testClusterDeploymentVariables(topology)
    val expectedVariables = testClusterKafkaClusterVariablesDefaults ++ testClusterKafkaClusterVariables
    actualVariables shouldBe expectedVariables
  }

  "a topology with environment variables for environments other than the kafka-cluster's" should "produce no additional variables" in {
    val topology = topologySourceSink().copy(environments = env("prod", vars("a", "1") ++ vars("b", "2", "3")))
    val actualVariables = testClusterDeploymentVariables(topology)
    val expectedVariables = testClusterKafkaClusterVariablesDefaults ++ testClusterKafkaClusterVariables
    actualVariables shouldBe expectedVariables
  }

  "a topology with environment variables of a single environment of the kafka-cluster" should "produce the variables" in {
    val topology = topologySourceSink().copy(environments = env("test-cluster", vars("a", "1") ++ vars("b", "2", "3")))
    val actualVariables = testClusterDeploymentVariables(topology)
    val expectedVariables = testClusterKafkaClusterVariablesDefaults ++ testClusterKafkaClusterVariables ++ Variables("a" -> Seq("1"), "b" -> Seq("2", "3"))
    actualVariables shouldBe expectedVariables
  }

  "a topology with environment variables of a multiple environments of the kafka-cluster" should "produce the variables" in {
    val topology = topologySourceSink().copy(
      environments =
        env("test", vars("a", "1") ++ vars("b", "2", "3")) ++
          env("test-cluster", vars("b", "20", "30") ++ vars("c", "something"))
    )
    val actualVariables = testClusterDeploymentVariables(topology)
    val expectedVariables = testClusterKafkaClusterVariablesDefaults ++ testClusterKafkaClusterVariables ++ Variables("a" -> Seq("1"), "b" -> Seq("20", "30"), "c" -> Seq("something"))
    actualVariables shouldBe expectedVariables
  }

  "a topology with environment variables of all the environments of the kafka-cluster" should "produce the variables" in {
    val topology = topologySourceSink().copy(
      environments =
        env("default", vars("a", "1")) ++
          env("test", vars("a", "100") ++ vars("b", "2", "3")) ++
          env("test-cluster", vars("b", "20", "30") ++ vars("c", "something"))
    )
    val actualVariables = testClusterDeploymentVariables(topology)
    val expectedVariables = testClusterKafkaClusterVariablesDefaults ++ testClusterKafkaClusterVariables ++ Variables("a" -> Seq("100"), "b" -> Seq("20", "30"), "c" -> Seq("something"))
    actualVariables shouldBe expectedVariables
  }

  "a topology with environment variables overriding the one in the kafka-cluster" should "produce the variables" in {
    val topology = topologySourceSink().copy(
      environments =
        env("default", vars("a", "1")) ++
          env("test", vars("a", "100") ++ vars("b", "2", "3")) ++
          env("test-cluster", vars("b", "20", "30") ++ vars("c", "something") ++ vars("env-specific", "XX"))
    )
    val actualVariables = testClusterDeploymentVariables(topology)
    val expectedVariables = testClusterKafkaClusterVariablesDefaults ++ Variables("a" -> Seq("100"), "b" -> Seq("20", "30"), "c" -> Seq("something"), "env-specific" -> Seq("XX"))
    actualVariables shouldBe expectedVariables
  }

  "a topology with environment variables overriding the one in the kafka-cluster but in the default environment" should "produce the variables" in {
    val topology = topologySourceSink().copy(
      environments =
        env("default", vars("a", "1") ++ vars("env-specific", "XX")) ++
          env("test", vars("a", "100") ++ vars("b", "2", "3")) ++
          env("test-cluster", vars("b", "20", "30") ++ vars("c", "something"))
    )
    val actualVariables = testClusterDeploymentVariables(topology)
    val expectedVariables = testClusterKafkaClusterVariablesDefaults ++ testClusterKafkaClusterVariables ++ Variables("a" -> Seq("100"), "b" -> Seq("20", "30"), "c" -> Seq("something"))
    actualVariables shouldBe expectedVariables
  }

  "a topology with environment variables overriding the one in the code defaults" should "produce the variables" in {
    val topology = topologySourceSink().copy(
      environments =
        env("default", vars("a", "1")) ++
          env("test", vars("a", "100") ++ vars("b", "2", "3")) ++
          env("test-cluster", vars("b", "20", "30") ++ vars("c", "something") ++ vars("env", "MyCustomEnv") ++ vars("short-env", "m"))
    )
    val actualVariables = testClusterDeploymentVariables(topology)
    val expectedVariables = testClusterKafkaClusterVariablesDefaults ++ testClusterKafkaClusterVariables ++ Variables("a" -> Seq("100"), "b" -> Seq("20", "30"), "c" -> Seq("something"), "env" -> Seq("MyCustomEnv"), "short-env" -> Seq("m"))
    actualVariables shouldBe expectedVariables
  }

  "a topology with environment variables overriding the one in the code defaults but in the default environment" should "produce the variables" in {
    val topology = topologySourceSink().copy(
      environments =
        env("default", vars("a", "1") ++ vars("env", "MyCustomEnv") ++ vars("short-env", "m")) ++
          env("test", vars("a", "100") ++ vars("b", "2", "3")) ++
          env("test-cluster", vars("b", "20", "30") ++ vars("c", "something"))
    )
    val actualVariables = testClusterDeploymentVariables(topology)
    val expectedVariables = testClusterKafkaClusterVariablesDefaults ++ testClusterKafkaClusterVariables ++ Variables("a" -> Seq("100"), "b" -> Seq("20", "30"), "c" -> Seq("something"))
    actualVariables shouldBe expectedVariables
  }
}
