/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.state

import com.mwam.kafkakewl.common.{AllStateEntities, ValidationResultMatchers}
import com.mwam.kafkakewl.common.AllStateEntities.InMemoryStateStores._
import com.mwam.kafkakewl.domain.EntityStateMetadata
import com.mwam.kafkakewl.domain.deploy.{Deployment, DeploymentStateChange, DeploymentTopologyVersion}
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaClusterAndTopology, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology.{Namespace, Topology, TopologyEntityId, TopologyStateChange}
import org.scalatest.{FlatSpec, Matchers}

class StateCommandProcessingValidatorSpec extends FlatSpec
  with Matchers
  with ValidationResultMatchers {

  def topologyNewVersion(user: String, topology: Topology, version: Int = 1) =
    TopologyStateChange.NewVersion(EntityStateMetadata(topology.topologyEntityId.id, version, user), topology)
  def deploymentNewVersion(user: String, deployment: Deployment, version: Int = 1) =
    DeploymentStateChange.NewVersion(EntityStateMetadata(KafkaClusterAndTopology.id(deployment.kafkaClusterId, deployment.topologyId), version, user), deployment)

  val testCluster = KafkaClusterEntityId("test-cluster")
  val topicDefaults: TopicDefaults = TopicDefaults()

  "deleting a topology when no deployment exists" should "be valid" in {
    val ss = AllStateEntities.InMemoryStateStores()
    val topologyId = TopologyEntityId("test")
    val topologyVersion = 1
    ss.topology.applyEntityStateChange(topologyNewVersion("user", Topology(Namespace("test")), topologyVersion))

    val actualValidationResult = StateCommandProcessingValidator.validateTopology(ss.toReadable, topologyId, None, topicDefaults)

    actualValidationResult should beValid
  }

  "deleting a topology when an EXACT deployment exists" should "be invalid" in {
    val ss = AllStateEntities.InMemoryStateStores()
    val topologyId = TopologyEntityId("test")
    val topologyVersion = 1
    val kafkaClusterId = testCluster
    ss.topology.applyEntityStateChange(topologyNewVersion("user", Topology(Namespace("test")), topologyVersion))
    ss.deployment.applyEntityStateChange(deploymentNewVersion("user", Deployment(kafkaClusterId, topologyId, DeploymentTopologyVersion.Exact(topologyVersion))))

    val actualValidationResult = StateCommandProcessingValidator.validateTopology(ss.toReadable, topologyId, None, topicDefaults)

    actualValidationResult should beInvalid
    actualValidationResult should containMessage("cannot delete topology 'test' because there are deployments in 'test-cluster' kafka-cluster")
  }

  "deleting a topology when a REMOVED deployment exists" should "be invalid" in {
    val ss = AllStateEntities.InMemoryStateStores()
    val topologyId = TopologyEntityId("test")
    val topologyVersion = 1
    val kafkaClusterId = testCluster
    ss.topology.applyEntityStateChange(topologyNewVersion("user", Topology(Namespace("test")), topologyVersion))
    ss.deployment.applyEntityStateChange(deploymentNewVersion("user", Deployment(kafkaClusterId, topologyId, DeploymentTopologyVersion.Remove())))

    val actualValidationResult = StateCommandProcessingValidator.validateTopology(ss.toReadable, topologyId, None, topicDefaults)

    actualValidationResult should beInvalid
    actualValidationResult should containMessage("cannot delete topology 'test' because there are deployments in 'test-cluster' kafka-cluster")
  }
}
