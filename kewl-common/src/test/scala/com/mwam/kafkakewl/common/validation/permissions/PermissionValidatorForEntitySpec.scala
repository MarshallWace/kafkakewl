/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation.permissions

import com.mwam.kafkakewl.common.{AllStateEntities, ValidationResultMatchers}
import com.mwam.kafkakewl.domain.deploy.{DeployedTopology, Deployment, DeploymentTopologyVersion}
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterAndTopology}
import com.mwam.kafkakewl.domain.permission.PermissionResourceOperation
import com.mwam.kafkakewl.domain.topology._
import com.mwam.kafkakewl.domain.EntityCompactors._
import com.mwam.kafkakewl.domain.metrics.{AggregatedConsumerGroupStatus, ConsumerGroupStatus, DeployedTopologyMetrics, DeployedTopologyMetricsCompact}
import org.scalatest.{FlatSpec, Matchers}
import cats.syntax.option._

class PermissionValidatorForEntitySpec extends FlatSpec
  with Matchers
  with ValidationResultMatchers
  with PermissionValidatorCommon {

  "super-users" should "see any permissions" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))

    validateEntity(ss, root, "perm1", kafkaClusterPermission(root, testCluster, PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, root, "perm1", kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, root, "perm1", kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, root, "perm1", kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, root, "perm1", namespacePermission(root, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, root, "perm1", namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, root, "perm1", namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, root, "perm1", namespacePermission(userZ, Namespace("test"), PermissionResourceOperation.Read)) should beValid

    compactAndValidateEntity(ss, root, "perm1", kafkaClusterPermission(root, testCluster, PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, root, "perm1", kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, root, "perm1", kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, root, "perm1", kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, root, "perm1", namespacePermission(root, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, root, "perm1", namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, root, "perm1", namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, root, "perm1", namespacePermission(userZ, Namespace("test"), PermissionResourceOperation.Read)) should beValid
  }

  "normal users with System:Read permission" should "see any permissions" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.permission.applyEntityStateChange(permissionNewVersion(root, systemPermission(userX, PermissionResourceOperation.Read)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, systemPermission(userY, PermissionResourceOperation.Write)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, systemPermission(userZ, PermissionResourceOperation.Any)))

    validateEntity(ss, userX, "perm1", kafkaClusterPermission(root, testCluster, PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userX, "perm1", kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userX, "perm1", kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userX, "perm1", kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userX, "perm1", namespacePermission(root, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userX, "perm1", namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userX, "perm1", namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userX, "perm1", namespacePermission(userZ, Namespace("test"), PermissionResourceOperation.Read)) should beValid

    validateEntity(ss, userY, "perm1", kafkaClusterPermission(root, testCluster, PermissionResourceOperation.Read)) should beInvalid
    validateEntity(ss, userY, "perm1", kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read)) should beInvalid
    validateEntity(ss, userY, "perm1", kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userY, "perm1", kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Read)) should beInvalid
    validateEntity(ss, userY, "perm1", namespacePermission(root, Namespace("test"), PermissionResourceOperation.Read)) should beInvalid
    validateEntity(ss, userY, "perm1", namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Read)) should beInvalid
    validateEntity(ss, userY, "perm1", namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userY, "perm1", namespacePermission(userZ, Namespace("test"), PermissionResourceOperation.Read)) should beInvalid

    validateEntity(ss, userZ, "perm1", kafkaClusterPermission(root, testCluster, PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userZ, "perm1", kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userZ, "perm1", kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userZ, "perm1", kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userZ, "perm1", namespacePermission(root, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userZ, "perm1", namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userZ, "perm1", namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userZ, "perm1", namespacePermission(userZ, Namespace("test"), PermissionResourceOperation.Read)) should beValid

    compactAndValidateEntity(ss, userX, "perm1", kafkaClusterPermission(root, testCluster, PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userX, "perm1", kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userX, "perm1", kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userX, "perm1", kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userX, "perm1", namespacePermission(root, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userX, "perm1", namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userX, "perm1", namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userX, "perm1", namespacePermission(userZ, Namespace("test"), PermissionResourceOperation.Read)) should beValid

    compactAndValidateEntity(ss, userY, "perm1", kafkaClusterPermission(root, testCluster, PermissionResourceOperation.Read)) should beInvalid
    compactAndValidateEntity(ss, userY, "perm1", kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read)) should beInvalid
    compactAndValidateEntity(ss, userY, "perm1", kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userY, "perm1", kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Read)) should beInvalid
    compactAndValidateEntity(ss, userY, "perm1", namespacePermission(root, Namespace("test"), PermissionResourceOperation.Read)) should beInvalid
    compactAndValidateEntity(ss, userY, "perm1", namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Read)) should beInvalid
    compactAndValidateEntity(ss, userY, "perm1", namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userY, "perm1", namespacePermission(userZ, Namespace("test"), PermissionResourceOperation.Read)) should beInvalid

    compactAndValidateEntity(ss, userZ, "perm1", kafkaClusterPermission(root, testCluster, PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userZ, "perm1", kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userZ, "perm1", kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userZ, "perm1", kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userZ, "perm1", namespacePermission(root, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userZ, "perm1", namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userZ, "perm1", namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userZ, "perm1", namespacePermission(userZ, Namespace("test"), PermissionResourceOperation.Read)) should beValid
  }

  "normal users" should "see their own permissions only" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))

    validateEntity(ss, userX, "perm1", kafkaClusterPermission(root, testCluster, PermissionResourceOperation.Read)) should beInvalid
    validateEntity(ss, userX, "perm1", kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userX, "perm1", kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read)) should beInvalid
    validateEntity(ss, userX, "perm1", kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Read)) should beInvalid
    validateEntity(ss, userX, "perm1", namespacePermission(root, Namespace("test"), PermissionResourceOperation.Read)) should beInvalid
    validateEntity(ss, userX, "perm1", namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userX, "perm1", namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Read)) should beInvalid
    validateEntity(ss, userX, "perm1", namespacePermission(userZ, Namespace("test"), PermissionResourceOperation.Read)) should beInvalid

    validateEntity(ss, userY, "perm1", kafkaClusterPermission(root, testCluster, PermissionResourceOperation.Read)) should beInvalid
    validateEntity(ss, userY, "perm1", kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read)) should beInvalid
    validateEntity(ss, userY, "perm1", kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userY, "perm1", kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Read)) should beInvalid
    validateEntity(ss, userY, "perm1", namespacePermission(root, Namespace("test"), PermissionResourceOperation.Read)) should beInvalid
    validateEntity(ss, userY, "perm1", namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Read)) should beInvalid
    validateEntity(ss, userY, "perm1", namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    validateEntity(ss, userY, "perm1", namespacePermission(userZ, Namespace("test"), PermissionResourceOperation.Read)) should beInvalid

    compactAndValidateEntity(ss, userX, "perm1", kafkaClusterPermission(root, testCluster, PermissionResourceOperation.Read)) should beInvalid
    compactAndValidateEntity(ss, userX, "perm1", kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userX, "perm1", kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read)) should beInvalid
    compactAndValidateEntity(ss, userX, "perm1", kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Read)) should beInvalid
    compactAndValidateEntity(ss, userX, "perm1", namespacePermission(root, Namespace("test"), PermissionResourceOperation.Read)) should beInvalid
    compactAndValidateEntity(ss, userX, "perm1", namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userX, "perm1", namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Read)) should beInvalid
    compactAndValidateEntity(ss, userX, "perm1", namespacePermission(userZ, Namespace("test"), PermissionResourceOperation.Read)) should beInvalid

    compactAndValidateEntity(ss, userY, "perm1", kafkaClusterPermission(root, testCluster, PermissionResourceOperation.Read)) should beInvalid
    compactAndValidateEntity(ss, userY, "perm1", kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read)) should beInvalid
    compactAndValidateEntity(ss, userY, "perm1", kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userY, "perm1", kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Read)) should beInvalid
    compactAndValidateEntity(ss, userY, "perm1", namespacePermission(root, Namespace("test"), PermissionResourceOperation.Read)) should beInvalid
    compactAndValidateEntity(ss, userY, "perm1", namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Read)) should beInvalid
    compactAndValidateEntity(ss, userY, "perm1", namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Read)) should beValid
    compactAndValidateEntity(ss, userY, "perm1", namespacePermission(userZ, Namespace("test"), PermissionResourceOperation.Read)) should beInvalid
  }

  "super-users" should "see any kafka-clusters" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))

    validateEntity(ss, root, testCluster.id, KafkaCluster(testCluster, "test-cluster-brokers")) should beValid
    validateEntity(ss, root, testCluster.id, KafkaCluster(prodCluster, "test-cluster-brokers")) should beValid

    compactAndValidateEntity(ss, root, testCluster.id, KafkaCluster(testCluster, "test-cluster-brokers")) should beValid
    compactAndValidateEntity(ss, root, testCluster.id, KafkaCluster(prodCluster, "test-cluster-brokers")) should beValid
  }

  "normal users" should "see any kafka-clusters for which they have READ permission" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Write)))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))

    validateEntity(ss, userX, testCluster.id, KafkaCluster(testCluster, "test-cluster-brokers")) should beValid
    validateEntity(ss, userY, testCluster.id, KafkaCluster(testCluster, "test-cluster-brokers")) should beInvalid
    validateEntity(ss, userZ, testCluster.id, KafkaCluster(testCluster, "test-cluster-brokers")) should beInvalid

    validateEntity(ss, userX, prodCluster.id, KafkaCluster(prodCluster, "prod-cluster-brokers")) should beInvalid
    validateEntity(ss, userY, prodCluster.id, KafkaCluster(prodCluster, "prod-cluster-brokers")) should beInvalid
    validateEntity(ss, userZ, prodCluster.id, KafkaCluster(prodCluster, "prod-cluster-brokers")) should beInvalid

    compactAndValidateEntity(ss, userX, testCluster.id, KafkaCluster(testCluster, "test-cluster-brokers")) should beValid
    compactAndValidateEntity(ss, userY, testCluster.id, KafkaCluster(testCluster, "test-cluster-brokers")) should beInvalid
    compactAndValidateEntity(ss, userZ, testCluster.id, KafkaCluster(testCluster, "test-cluster-brokers")) should beInvalid

    compactAndValidateEntity(ss, userX, prodCluster.id, KafkaCluster(prodCluster, "prod-cluster-brokers")) should beInvalid
    compactAndValidateEntity(ss, userY, prodCluster.id, KafkaCluster(prodCluster, "prod-cluster-brokers")) should beInvalid
    compactAndValidateEntity(ss, userZ, prodCluster.id, KafkaCluster(prodCluster, "prod-cluster-brokers")) should beInvalid
  }

  "super-users" should "see any topologies" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))

    validateEntity(ss, root, "test", Topology(Namespace("test"))) should beValid
    compactAndValidateEntity(ss, root, "test", Topology(Namespace("test"))) should beValid
  }

  "normal users" should "see any topologies for which they have READ permission" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Read)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Write)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userZ, Namespace("test2"), PermissionResourceOperation.Read)))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))

    validateEntity(ss, userX, "test", Topology(Namespace("test"))) should beValid
    validateEntity(ss, userY, "test", Topology(Namespace("test"))) should beInvalid
    validateEntity(ss, userZ, "test", Topology(Namespace("test"))) should beInvalid
    compactAndValidateEntity(ss, userX, "test", Topology(Namespace("test"))) should beValid
    compactAndValidateEntity(ss, userY, "test", Topology(Namespace("test"))) should beInvalid
    compactAndValidateEntity(ss, userZ, "test", Topology(Namespace("test"))) should beInvalid
  }

  "super-users" should "see any resolved topologies" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))

    validateEntity(ss, root, "test", Topology.ResolvedTopology(Namespace("test"), TopologyId(""))) should beValid
  }

  "normal users" should "see any resolved topologies for which they have READ permission" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Read)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Write)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userZ, Namespace("test2"), PermissionResourceOperation.Read)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, topologyPermission(userW, TopologyEntityId("test"), PermissionResourceOperation.Read)))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))

    validateEntity(ss, userX, "test", Topology.ResolvedTopology(Namespace("test"), TopologyId(""))) should beValid
    validateEntity(ss, userY, "test", Topology.ResolvedTopology(Namespace("test"), TopologyId(""))) should beInvalid
    validateEntity(ss, userZ, "test", Topology.ResolvedTopology(Namespace("test"), TopologyId(""))) should beInvalid
    validateEntity(ss, userW, "test", Topology.ResolvedTopology(Namespace("test"), TopologyId(""))) should beValid
  }

  "super-users" should "see any deployments" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))

    validateEntity(ss, root, KafkaClusterAndTopology.id(testCluster, testTopologyId), Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))) should beValid
    compactAndValidateEntity(ss, root, KafkaClusterAndTopology.id(testCluster, testTopologyId), Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))) should beValid
  }

  "normal users" should "see any deployments with READ namespace and kafka-cluster permission" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Write)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Read)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Write)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userZ, Namespace("test2"), PermissionResourceOperation.Read)))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))

    validateEntity(ss, userX, KafkaClusterAndTopology.id(testCluster, testTopologyId), Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))) should beValid
    validateEntity(ss, userY, KafkaClusterAndTopology.id(testCluster, testTopologyId), Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))) should beInvalid
    validateEntity(ss, userZ, KafkaClusterAndTopology.id(testCluster, testTopologyId), Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))) should beInvalid
    compactAndValidateEntity(ss, userX, KafkaClusterAndTopology.id(testCluster, testTopologyId), Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))) should beValid
    compactAndValidateEntity(ss, userY, KafkaClusterAndTopology.id(testCluster, testTopologyId), Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))) should beInvalid
    compactAndValidateEntity(ss, userZ, KafkaClusterAndTopology.id(testCluster, testTopologyId), Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))) should beInvalid
  }

  "super-users" should "see any deployedtopologies" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))

    validateEntity(ss, root, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopology(testCluster, testTopologyId, 1, None, true)) should beValid
    compactAndValidateEntity(ss, root, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopology(testCluster, testTopologyId, 1, None, true)) should beValid
  }

  "super-users" should "see any deployedtopologies metrics" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))

    validateEntity(ss, root, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopologyMetrics(testCluster, testTopologyId, ConsumerGroupStatus.Ok, AggregatedConsumerGroupStatus.Ok.some, Map.empty, Map.empty)) should beValid
    validateEntity(ss, root, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopologyMetricsCompact(testCluster, testTopologyId, ConsumerGroupStatus.Ok, AggregatedConsumerGroupStatus.Ok.some)) should beValid
  }

  "normal users" should "see any deployedtopologies with READ namespace and kafka-cluster permission" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userW, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Write)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Read)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Write)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userZ, Namespace("test2"), PermissionResourceOperation.Read)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, topologyPermission(userW, TopologyEntityId("test"), PermissionResourceOperation.Read)))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))

    validateEntity(ss, userX, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopology(testCluster, testTopologyId, 1, None, true)) should beValid
    validateEntity(ss, userY, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopology(testCluster, testTopologyId, 1, None, true)) should beInvalid
    validateEntity(ss, userZ, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopology(testCluster, testTopologyId, 1, None, true)) should beInvalid
    validateEntity(ss, userW, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopology(testCluster, testTopologyId, 1, None, true)) should beValid
    compactAndValidateEntity(ss, userX, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopology(testCluster, testTopologyId, 1, None, true)) should beValid
    compactAndValidateEntity(ss, userY, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopology(testCluster, testTopologyId, 1, None, true)) should beInvalid
    compactAndValidateEntity(ss, userZ, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopology(testCluster, testTopologyId, 1, None, true)) should beInvalid
    compactAndValidateEntity(ss, userW, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopology(testCluster, testTopologyId, 1, None, true)) should beValid
  }

  "normal users" should "see any deployedtopologies metrics with READ namespace and kafka-cluster permission" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userW, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Write)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Read)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Write)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userZ, Namespace("test2"), PermissionResourceOperation.Read)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, topologyPermission(userW, TopologyEntityId("test"), PermissionResourceOperation.Read)))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))

    validateEntity(ss, userX, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopologyMetrics(testCluster, testTopologyId, ConsumerGroupStatus.Ok, AggregatedConsumerGroupStatus.Ok.some, Map.empty, Map.empty)) should beValid
    validateEntity(ss, userY, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopologyMetrics(testCluster, testTopologyId, ConsumerGroupStatus.Ok, AggregatedConsumerGroupStatus.Ok.some, Map.empty, Map.empty)) should beInvalid
    validateEntity(ss, userZ, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopologyMetrics(testCluster, testTopologyId, ConsumerGroupStatus.Ok, AggregatedConsumerGroupStatus.Ok.some, Map.empty, Map.empty)) should beInvalid
    validateEntity(ss, userW, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopologyMetrics(testCluster, testTopologyId, ConsumerGroupStatus.Ok, AggregatedConsumerGroupStatus.Ok.some, Map.empty, Map.empty)) should beValid
    validateEntity(ss, userX, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopologyMetricsCompact(testCluster, testTopologyId, ConsumerGroupStatus.Ok, AggregatedConsumerGroupStatus.Ok.some)) should beValid
    validateEntity(ss, userY, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopologyMetricsCompact(testCluster, testTopologyId, ConsumerGroupStatus.Ok, AggregatedConsumerGroupStatus.Ok.some)) should beInvalid
    validateEntity(ss, userZ, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopologyMetricsCompact(testCluster, testTopologyId, ConsumerGroupStatus.Ok, AggregatedConsumerGroupStatus.Ok.some)) should beInvalid
    validateEntity(ss, userW, KafkaClusterAndTopology.id(testCluster, testTopologyId), DeployedTopologyMetricsCompact(testCluster, testTopologyId, ConsumerGroupStatus.Ok, AggregatedConsumerGroupStatus.Ok.some)) should beValid
  }

  "super-users" should "see any resolved deployedtopologies" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))

    validateEntity(ss, root, KafkaClusterAndTopology.id(testCluster, testTopologyId), TopologyToDeploy.ResolvedTopology(Namespace("test"), TopologyId(""))) should beValid
  }

  "normal users" should "see any resolved deployedtopologies with READ namespace and kafka-cluster permission" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userW, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Write)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Read)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Write)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userZ, Namespace("test2"), PermissionResourceOperation.Read)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, topologyPermission(userW, TopologyEntityId("test"), PermissionResourceOperation.Read)))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))

    validateEntity(ss, userX, KafkaClusterAndTopology.id(testCluster, testTopologyId), TopologyToDeploy.ResolvedTopology(Namespace("test"), TopologyId(""))) should beValid
    validateEntity(ss, userY, KafkaClusterAndTopology.id(testCluster, testTopologyId), TopologyToDeploy.ResolvedTopology(Namespace("test"), TopologyId(""))) should beInvalid
    validateEntity(ss, userZ, KafkaClusterAndTopology.id(testCluster, testTopologyId), TopologyToDeploy.ResolvedTopology(Namespace("test"), TopologyId(""))) should beInvalid
    validateEntity(ss, userW, KafkaClusterAndTopology.id(testCluster, testTopologyId), TopologyToDeploy.ResolvedTopology(Namespace("test"), TopologyId(""))) should beValid
  }
}
