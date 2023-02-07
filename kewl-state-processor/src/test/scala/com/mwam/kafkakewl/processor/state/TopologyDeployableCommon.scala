/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.state

import com.mwam.kafkakewl.domain.Expressions.Variables
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.topology.{Topology, TopologyToDeploy}
import com.mwam.kafkakewl.domain.{DeploymentEnvironment, DeploymentEnvironmentId, DeploymentEnvironments}
import com.mwam.kafkakewl.processor.state.TopologyDeployable.MakeDeployableResult

trait TopologyDeployableCommon {
  val testClusterKafkaClusterId = KafkaClusterEntityId("test-cluster")
  val testClusterKafkaCluster = KafkaCluster(
    KafkaClusterEntityId("test-cluster"),
    brokers = "brokers",
    environments = DeploymentEnvironments.OrderedVariables(
      DeploymentEnvironmentId("default") -> Map.empty,
      DeploymentEnvironmentId("test") -> Map("env-specific" -> DeploymentEnvironment.Variable.Value(Seq("TT"))),
      DeploymentEnvironmentId("test-cluster") -> Map.empty
    )
  )
  // this comes from above
  val testClusterKafkaClusterVariables = Map(
    "env-specific" -> Seq("TT")
  )
  // this should come from DeploymentEnvironments.Variables.defaults
  val testClusterKafkaClusterVariablesDefaults = Map(
    "env" -> Seq("test"),
    "short-env" -> Seq("t"),
    "developers-access" -> Seq("Full"),
    "deploy-with-authorization-code" -> Seq("false")
  )

  val prodCluster = KafkaClusterEntityId("prod-cluster")
  val prodClusterKafkaCluster = KafkaCluster(
    KafkaClusterEntityId("prod-cluster"),
    brokers = "brokers",
    environments = DeploymentEnvironments.OrderedVariables(
      DeploymentEnvironmentId("default") -> Map.empty,
      DeploymentEnvironmentId("prod") -> Map("env-specific" -> DeploymentEnvironment.Variable.Value(Seq("PP"))),
      DeploymentEnvironmentId("prod-cluster") -> Map.empty
    )
  )
  // this comes from above
  val prodClusterKafkaClusterVariables = Map(
    "env-specific" -> Seq("PP")
  )
  // this should come from DeploymentEnvironments.Variables.defaults
  val prodClusterKafkaClusterVariablesDefaults = Map(
    "env" -> Seq("prod"),
    "short-env" -> Seq("p"),
    "developers-access" -> Seq("TopicReadOnly"),
    "deploy-with-authorization-code" -> Seq("true")
  )

  def testClusterDeploymentVariables(topology: Topology): Variables =
    TopologyDeployable.deploymentVariables(testClusterKafkaClusterId, testClusterKafkaCluster, topology)
  def testClusterMakeDeployable(topology: Topology): MakeDeployableResult[TopologyToDeploy] =
    TopologyDeployable.makeTopologyDeployable(testClusterDeploymentVariables(topology), topology)

  def prodClusterDeploymentVariables(topology: Topology): Variables =
    TopologyDeployable.deploymentVariables(prodCluster, prodClusterKafkaCluster, topology)
  def prodClusterMakeDeployable(topology: Topology): MakeDeployableResult[TopologyToDeploy] =
    TopologyDeployable.makeTopologyDeployable(prodClusterDeploymentVariables(topology), topology)

  def env(envId: String, vars: DeploymentEnvironment.Variables) = DeploymentEnvironments.Variables(DeploymentEnvironmentId(envId) -> vars)
  def vars(key: String, value: String*) = DeploymentEnvironment.Variables(key -> DeploymentEnvironment.Variable.Value(value.toSeq))
}
