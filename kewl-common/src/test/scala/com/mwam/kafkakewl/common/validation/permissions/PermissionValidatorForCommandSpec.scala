/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation.permissions

import com.mwam.kafkakewl.common.{AllStateEntities, ValidationResultMatchers}
import com.mwam.kafkakewl.domain.deploy._
import com.mwam.kafkakewl.domain.kafkacluster.KafkaCluster
import com.mwam.kafkakewl.domain.permission._
import com.mwam.kafkakewl.domain.topology._
import com.mwam.kafkakewl.domain.{Command, FlexibleName}
import org.scalatest.{FlatSpec, Matchers}

class PermissionValidatorForCommandSpec extends FlatSpec
  with Matchers
  with ValidationResultMatchers
  with PermissionValidatorCommon {

  "PermissionGetAll" should "be possible for any users" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))

    validateCommand(ss, Command.PermissionGetAll(commandMetadata(root), compact = false)) should beValid
    validateCommand(ss, Command.PermissionGetAll(commandMetadata(userX), compact = false)) should beValid
    validateCommand(ss, Command.PermissionGetAll(commandMetadata(userY), compact = false)) should beValid
    validateCommand(ss, Command.PermissionGetAll(commandMetadata(userZ), compact = false)) should beValid
  }

  "PermissionGet" should "be possible for any users" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy), permissionId = PermissionEntityId("perm1")))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy), permissionId = PermissionEntityId("perm2")))

    validateCommand(ss, Command.PermissionGet(commandMetadata(root), PermissionEntityId("perm1"), compact = false)) should beValid
    validateCommand(ss, Command.PermissionGet(commandMetadata(root), PermissionEntityId("perm2"), compact = false)) should beValid
    validateCommand(ss, Command.PermissionGet(commandMetadata(root), PermissionEntityId("perm-invalid"), compact = false)) should beValid
    validateCommand(ss, Command.PermissionGet(commandMetadata(userX), PermissionEntityId("perm1"), compact = false)) should beValid
    validateCommand(ss, Command.PermissionGet(commandMetadata(userX), PermissionEntityId("perm2"), compact = false)) should beValid
    validateCommand(ss, Command.PermissionGet(commandMetadata(userX), PermissionEntityId("perm-invalid"), compact = false)) should beValid
    validateCommand(ss, Command.PermissionGet(commandMetadata(userY), PermissionEntityId("perm1"), compact = false)) should beValid
    validateCommand(ss, Command.PermissionGet(commandMetadata(userY), PermissionEntityId("perm2"), compact = false)) should beValid
    validateCommand(ss, Command.PermissionGet(commandMetadata(userY), PermissionEntityId("perm-invalid"), compact = false)) should beValid
    validateCommand(ss, Command.PermissionGet(commandMetadata(userZ), PermissionEntityId("perm1"), compact = false)) should beValid
    validateCommand(ss, Command.PermissionGet(commandMetadata(userZ), PermissionEntityId("perm2"), compact = false)) should beValid
    validateCommand(ss, Command.PermissionGet(commandMetadata(userZ), PermissionEntityId("perm-invalid"), compact = false)) should beValid
  }

  "Manipulating permissions" should "only be possible for super-users" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()

    validateCommand(ss, Command.PermissionCreate(commandMetadata(root), PermissionEntityId("perm1"), kafkaClusterPermission(root, testCluster, PermissionResourceOperation.Read))) should beValid
    validateCommand(ss, Command.PermissionCreate(commandMetadata(root), PermissionEntityId("perm1"), kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read))) should beValid
    validateCommand(ss, Command.PermissionCreate(commandMetadata(userX), PermissionEntityId("perm1"), kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read))) should beInvalid
    validateCommand(ss, Command.PermissionCreate(commandMetadata(userX), PermissionEntityId("perm1"), kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read))) should beInvalid

    validateCommand(ss, Command.PermissionCreateWithContent(commandMetadata(root), kafkaClusterPermission(root, testCluster, PermissionResourceOperation.Read))) should beValid
    validateCommand(ss, Command.PermissionCreateWithContent(commandMetadata(root), kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read))) should beValid
    validateCommand(ss, Command.PermissionCreateWithContent(commandMetadata(userX), kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read))) should beInvalid
    validateCommand(ss, Command.PermissionCreateWithContent(commandMetadata(userX), kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read))) should beInvalid

    validateCommand(ss, Command.PermissionUpdate(commandMetadata(root), PermissionEntityId("perm1"), kafkaClusterPermission(root, testCluster, PermissionResourceOperation.Read))) should beValid
    validateCommand(ss, Command.PermissionUpdate(commandMetadata(root), PermissionEntityId("perm1"), kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read))) should beValid
    validateCommand(ss, Command.PermissionUpdate(commandMetadata(userX), PermissionEntityId("perm1"), kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read))) should beInvalid
    validateCommand(ss, Command.PermissionUpdate(commandMetadata(userX), PermissionEntityId("perm1"), kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read))) should beInvalid

    validateCommand(ss, Command.PermissionDelete(commandMetadata(root), PermissionEntityId("perm1"))) should beValid
    validateCommand(ss, Command.PermissionDelete(commandMetadata(userX), PermissionEntityId("perm1"))) should beInvalid

    validateCommand(ss, Command.PermissionDeleteByContent(commandMetadata(root), kafkaClusterPermission(root, testCluster, PermissionResourceOperation.Read))) should beValid
    validateCommand(ss, Command.PermissionDeleteByContent(commandMetadata(root), kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read))) should beValid
    validateCommand(ss, Command.PermissionDeleteByContent(commandMetadata(userX), kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read))) should beInvalid
    validateCommand(ss, Command.PermissionDeleteByContent(commandMetadata(userX), kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read))) should beInvalid
  }

  "KafkaClusterGetAll" should "be possible for any users" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))

    validateCommand(ss, Command.KafkaClusterGetAll(commandMetadata(root), compact = false)) should beValid
    validateCommand(ss, Command.KafkaClusterGetAll(commandMetadata(userX), compact = false)) should beValid
    validateCommand(ss, Command.KafkaClusterGetAll(commandMetadata(userY), compact = false)) should beValid
    validateCommand(ss, Command.KafkaClusterGetAll(commandMetadata(userZ), compact = false)) should beValid
  }

  "KafkaClusterGet" should "only be possible for super-users and users with READ kafka-cluster permission" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Deploy)))

    validateCommand(ss, Command.KafkaClusterGet(commandMetadata(root), testCluster, compact = false)) should beValid
    validateCommand(ss, Command.KafkaClusterGet(commandMetadata(userX), testCluster, compact = false)) should beValid
    validateCommand(ss, Command.KafkaClusterGet(commandMetadata(userY), testCluster, compact = false)) should beValid
    validateCommand(ss, Command.KafkaClusterGet(commandMetadata(userZ), testCluster, compact = false)) should beInvalid
    validateCommand(ss, Command.KafkaClusterGet(commandMetadata(root), prodCluster, compact = false)) should beValid
    validateCommand(ss, Command.KafkaClusterGet(commandMetadata(userX), prodCluster, compact = false)) should beInvalid
    validateCommand(ss, Command.KafkaClusterGet(commandMetadata(userY), prodCluster, compact = false)) should beInvalid
    validateCommand(ss, Command.KafkaClusterGet(commandMetadata(userZ), prodCluster, compact = false)) should beInvalid
  }

  "Manipulating kafka-clusters" should "only be possible for super-users and users with WRITE kafka-cluster permission" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Write, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Deploy)))

    validateCommand(ss, Command.KafkaClusterCreate(commandMetadata(root), testCluster,  KafkaCluster(testCluster, "test-cluster-brokers"))) should beValid
    validateCommand(ss, Command.KafkaClusterCreate(commandMetadata(userX), testCluster,  KafkaCluster(testCluster, "test-cluster-brokers"))) should beValid
    validateCommand(ss, Command.KafkaClusterCreate(commandMetadata(userY), testCluster,  KafkaCluster(testCluster, "test-cluster-brokers"))) should beInvalid
    validateCommand(ss, Command.KafkaClusterCreate(commandMetadata(userZ), testCluster,  KafkaCluster(testCluster, "test-cluster-brokers"))) should beInvalid
    validateCommand(ss, Command.KafkaClusterCreate(commandMetadata(root), prodCluster,  KafkaCluster(prodCluster, "prod-cluster-brokers"))) should beValid
    validateCommand(ss, Command.KafkaClusterCreate(commandMetadata(userX), prodCluster,  KafkaCluster(prodCluster, "prod-cluster-brokers"))) should beInvalid
    validateCommand(ss, Command.KafkaClusterCreate(commandMetadata(userY), prodCluster,  KafkaCluster(prodCluster, "prod-cluster-brokers"))) should beInvalid
    validateCommand(ss, Command.KafkaClusterCreate(commandMetadata(userZ), prodCluster,  KafkaCluster(prodCluster, "prod-cluster-brokers"))) should beInvalid

    validateCommand(ss, Command.KafkaClusterUpdate(commandMetadata(root), testCluster,  KafkaCluster(testCluster, "test-cluster-brokers"))) should beValid
    validateCommand(ss, Command.KafkaClusterUpdate(commandMetadata(userX), testCluster,  KafkaCluster(testCluster, "test-cluster-brokers"))) should beValid
    validateCommand(ss, Command.KafkaClusterUpdate(commandMetadata(userY), testCluster,  KafkaCluster(testCluster, "test-cluster-brokers"))) should beInvalid
    validateCommand(ss, Command.KafkaClusterUpdate(commandMetadata(userZ), testCluster,  KafkaCluster(testCluster, "test-cluster-brokers"))) should beInvalid
    validateCommand(ss, Command.KafkaClusterUpdate(commandMetadata(root), prodCluster,  KafkaCluster(prodCluster, "prod-cluster-brokers"))) should beValid
    validateCommand(ss, Command.KafkaClusterUpdate(commandMetadata(userX), prodCluster,  KafkaCluster(prodCluster, "prod-cluster-brokers"))) should beInvalid
    validateCommand(ss, Command.KafkaClusterUpdate(commandMetadata(userY), prodCluster,  KafkaCluster(prodCluster, "prod-cluster-brokers"))) should beInvalid
    validateCommand(ss, Command.KafkaClusterUpdate(commandMetadata(userZ), prodCluster,  KafkaCluster(prodCluster, "prod-cluster-brokers"))) should beInvalid

    validateCommand(ss, Command.KafkaClusterDelete(commandMetadata(root), testCluster)) should beValid
    validateCommand(ss, Command.KafkaClusterDelete(commandMetadata(userX), testCluster)) should beValid
    validateCommand(ss, Command.KafkaClusterDelete(commandMetadata(userY), testCluster)) should beInvalid
    validateCommand(ss, Command.KafkaClusterDelete(commandMetadata(userZ), testCluster)) should beInvalid
    validateCommand(ss, Command.KafkaClusterDelete(commandMetadata(root), prodCluster)) should beValid
    validateCommand(ss, Command.KafkaClusterDelete(commandMetadata(userX), prodCluster)) should beInvalid
    validateCommand(ss, Command.KafkaClusterDelete(commandMetadata(userY), prodCluster)) should beInvalid
    validateCommand(ss, Command.KafkaClusterDelete(commandMetadata(userZ), prodCluster)) should beInvalid
  }

  "TopologyGetAll" should "be possible for any users" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))

    validateCommand(ss, Command.TopologyGetAll(commandMetadata(root), compact = false)) should beValid
    validateCommand(ss, Command.TopologyGetAll(commandMetadata(userX), compact = false)) should beValid
    validateCommand(ss, Command.TopologyGetAll(commandMetadata(userY), compact = false)) should beValid
    validateCommand(ss, Command.TopologyGetAll(commandMetadata(userZ), compact = false)) should beValid
  }

  "TopologyGet" should "only be possible for super-users and users with READ permission" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Read)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Write)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userZ, Namespace("test2"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, topologyPermission(userW, TopologyEntityId("test"), PermissionResourceOperation.Read)))

    validateCommand(ss, Command.TopologyGet(commandMetadata(root), testTopologyId, compact = false)) should beValid
    validateCommand(ss, Command.TopologyGet(commandMetadata(userX), testTopologyId, compact = false)) should beValid
    validateCommand(ss, Command.TopologyGet(commandMetadata(userY), testTopologyId, compact = false)) should beInvalid
    validateCommand(ss, Command.TopologyGet(commandMetadata(userZ), testTopologyId, compact = false)) should beInvalid
    validateCommand(ss, Command.TopologyGet(commandMetadata(userW), testTopologyId, compact = false)) should beValid
  }

  "Manipulating topologies" should "only be possible for super-users and users with WRITE namespace permission" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Write)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Read)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userZ, Namespace("test2"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, topologyPermission(userW, TopologyEntityId("test"), PermissionResourceOperation.Write)))

    validateCommand(ss, Command.TopologyCreate(commandMetadata(root), testTopologyId,  Topology(Namespace("test")))) should beValid
    validateCommand(ss, Command.TopologyCreate(commandMetadata(userX), testTopologyId, Topology(Namespace("test")))) should beValid
    validateCommand(ss, Command.TopologyCreate(commandMetadata(userY), testTopologyId, Topology(Namespace("test")))) should beInvalid
    validateCommand(ss, Command.TopologyCreate(commandMetadata(userZ), testTopologyId, Topology(Namespace("test")))) should beInvalid
    validateCommand(ss, Command.TopologyCreate(commandMetadata(userW), testTopologyId, Topology(Namespace("test")))) should beValid

    validateCommand(ss, Command.TopologyUpdate(commandMetadata(root), testTopologyId,  Topology(Namespace("test")))) should beValid
    validateCommand(ss, Command.TopologyUpdate(commandMetadata(userX), testTopologyId, Topology(Namespace("test")))) should beValid
    validateCommand(ss, Command.TopologyUpdate(commandMetadata(userY), testTopologyId, Topology(Namespace("test")))) should beInvalid
    validateCommand(ss, Command.TopologyUpdate(commandMetadata(userZ), testTopologyId, Topology(Namespace("test")))) should beInvalid
    validateCommand(ss, Command.TopologyUpdate(commandMetadata(userW), testTopologyId, Topology(Namespace("test")))) should beValid

    validateCommand(ss, Command.TopologyDelete(commandMetadata(root), TopologyEntityId("test"))) should beValid
    validateCommand(ss, Command.TopologyDelete(commandMetadata(userX), TopologyEntityId("test"))) should beValid
    validateCommand(ss, Command.TopologyDelete(commandMetadata(userY), TopologyEntityId("test"))) should beInvalid
    validateCommand(ss, Command.TopologyDelete(commandMetadata(userZ), TopologyEntityId("test"))) should beInvalid
    validateCommand(ss, Command.TopologyDelete(commandMetadata(userW), TopologyEntityId("test"))) should beValid
  }

  "DeploymentGetAll" should "be possible for any users" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))
    ss.deployment.applyEntityStateChange(deploymentNewVersion(userX, Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))))

    validateCommand(ss, Command.DeploymentGetAll(commandMetadata(root), compact = false)) should beValid
    validateCommand(ss, Command.DeploymentGetAll(commandMetadata(userX), compact = false)) should beValid
    validateCommand(ss, Command.DeploymentGetAll(commandMetadata(userY), compact = false)) should beValid
    validateCommand(ss, Command.DeploymentGetAll(commandMetadata(userZ), compact = false)) should beValid
  }

  "DeploymentGet" should "only be possible for super-users and users with READ kafka-cluster and READ namespace permissions" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))
    ss.deployment.applyEntityStateChange(deploymentNewVersion(userX, Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, topologyPermission(userW, TopologyEntityId("test"), PermissionResourceOperation.Read)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userW, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))

    validateCommand(ss, Command.DeploymentGet(commandMetadata(root), testCluster, testTopologyId, compact = false)) should beValid
    validateCommand(ss, Command.DeploymentGet(commandMetadata(userX), testCluster, testTopologyId, compact = false)) should beValid
    validateCommand(ss, Command.DeploymentGet(commandMetadata(userY), testCluster, testTopologyId, compact = false)) should beInvalid
    validateCommand(ss, Command.DeploymentGet(commandMetadata(userZ), testCluster, testTopologyId, compact = false)) should beInvalid
    validateCommand(ss, Command.DeploymentGet(commandMetadata(userW), testCluster, testTopologyId, compact = false)) should beValid
  }

  "Manipulating deployments" should "only be possible for super-users and users with DEPLOY kafka-cluster and DEPLOY namespace permissions" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))
    ss.deployment.applyEntityStateChange(deploymentNewVersion(userX, Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userW, Namespace("test"), PermissionResourceOperation.Read)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userW, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, topologyPermission(userV, TopologyEntityId("test"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userV, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))

    validateCommand(ss, Command.DeploymentCreate(commandMetadata(root), testCluster, testTopologyId, DeploymentChange(testCluster, TopologyEntityId("test")))) should beValid
    validateCommand(ss, Command.DeploymentCreate(commandMetadata(userX), testCluster, testTopologyId, DeploymentChange(testCluster, TopologyEntityId("test")))) should beValid
    validateCommand(ss, Command.DeploymentCreate(commandMetadata(userY), testCluster, testTopologyId, DeploymentChange(testCluster, TopologyEntityId("test")))) should beInvalid
    validateCommand(ss, Command.DeploymentCreate(commandMetadata(userZ), testCluster, testTopologyId, DeploymentChange(testCluster, TopologyEntityId("test")))) should beInvalid
    validateCommand(ss, Command.DeploymentCreate(commandMetadata(userW), testCluster, testTopologyId, DeploymentChange(testCluster, TopologyEntityId("test")))) should beInvalid
    validateCommand(ss, Command.DeploymentCreate(commandMetadata(userV), testCluster, testTopologyId, DeploymentChange(testCluster, TopologyEntityId("test")))) should beValid

    validateCommand(ss, Command.DeploymentUpdate(commandMetadata(root), testCluster, testTopologyId, DeploymentChange(testCluster, TopologyEntityId("test")))) should beValid
    validateCommand(ss, Command.DeploymentUpdate(commandMetadata(userX), testCluster, testTopologyId, DeploymentChange(testCluster, TopologyEntityId("test")))) should beValid
    validateCommand(ss, Command.DeploymentUpdate(commandMetadata(userY), testCluster, testTopologyId, DeploymentChange(testCluster, TopologyEntityId("test")))) should beInvalid
    validateCommand(ss, Command.DeploymentUpdate(commandMetadata(userZ), testCluster, testTopologyId, DeploymentChange(testCluster, TopologyEntityId("test")))) should beInvalid
    validateCommand(ss, Command.DeploymentUpdate(commandMetadata(userW), testCluster, testTopologyId, DeploymentChange(testCluster, TopologyEntityId("test")))) should beInvalid
    validateCommand(ss, Command.DeploymentUpdate(commandMetadata(userV), testCluster, testTopologyId, DeploymentChange(testCluster, TopologyEntityId("test")))) should beValid

    validateCommand(ss, Command.DeploymentReApply(commandMetadata(root), testCluster, testTopologyId, DeploymentOptions())) should beValid
    validateCommand(ss, Command.DeploymentReApply(commandMetadata(userX), testCluster, testTopologyId, DeploymentOptions())) should beValid
    validateCommand(ss, Command.DeploymentReApply(commandMetadata(userY), testCluster, testTopologyId, DeploymentOptions())) should beInvalid
    validateCommand(ss, Command.DeploymentReApply(commandMetadata(userZ), testCluster, testTopologyId, DeploymentOptions())) should beInvalid
    validateCommand(ss, Command.DeploymentReApply(commandMetadata(userW), testCluster, testTopologyId, DeploymentOptions())) should beInvalid
    validateCommand(ss, Command.DeploymentReApply(commandMetadata(userV), testCluster, testTopologyId, DeploymentOptions())) should beValid

    validateCommand(ss, Command.DeploymentDelete(commandMetadata(root), testCluster, TopologyEntityId("test"))) should beValid
    validateCommand(ss, Command.DeploymentDelete(commandMetadata(userX), testCluster, TopologyEntityId("test"))) should beValid
    validateCommand(ss, Command.DeploymentDelete(commandMetadata(userY), testCluster, TopologyEntityId("test"))) should beInvalid
    validateCommand(ss, Command.DeploymentDelete(commandMetadata(userZ), testCluster, TopologyEntityId("test"))) should beInvalid
    validateCommand(ss, Command.DeploymentDelete(commandMetadata(userW), testCluster, TopologyEntityId("test"))) should beInvalid
    validateCommand(ss, Command.DeploymentDelete(commandMetadata(userV), testCluster, TopologyEntityId("test"))) should beValid
  }

  "DeployedTopologyGetKafkaClustersAll" should "be possible for any users" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))
    ss.deployment.applyEntityStateChange(deploymentNewVersion(userX, Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read)))

    validateCommand(ss, Command.DeployedTopologyGetKafkaClustersAll(commandMetadata(root), compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyGetKafkaClustersAll(commandMetadata(userX), compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyGetKafkaClustersAll(commandMetadata(userY), compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyGetKafkaClustersAll(commandMetadata(userZ), compact = false)) should beValid

    validateCommand(ss, Command.DeployedTopologyGetKafkaClustersAll(commandMetadata(root), compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyGetKafkaClustersAll(commandMetadata(userX), compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyGetKafkaClustersAll(commandMetadata(userY), compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyGetKafkaClustersAll(commandMetadata(userZ), compact = false)) should beValid
  }

  "DeployedTopologyMetricsGetKafkaClustersAll" should "be possible for any users" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))
    ss.deployment.applyEntityStateChange(deploymentNewVersion(userX, Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read)))

    validateCommand(ss, Command.DeployedTopologyMetricsGetKafkaClustersAll(commandMetadata(root), compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyMetricsGetKafkaClustersAll(commandMetadata(userX), compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyMetricsGetKafkaClustersAll(commandMetadata(userY), compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyMetricsGetKafkaClustersAll(commandMetadata(userZ), compact = false)) should beValid

    validateCommand(ss, Command.DeployedTopologyMetricsGetKafkaClustersAll(commandMetadata(root), compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyMetricsGetKafkaClustersAll(commandMetadata(userX), compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyMetricsGetKafkaClustersAll(commandMetadata(userY), compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyMetricsGetKafkaClustersAll(commandMetadata(userZ), compact = false)) should beValid
  }

  "DeployedTopologyGetAll" should "only be possible for super-users and users with READ kafka-cluster permission" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))
    ss.deployment.applyEntityStateChange(deploymentNewVersion(userX, Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read)))

    validateCommand(ss, Command.DeployedTopologyGetAll(commandMetadata(root), testCluster, compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyGetAll(commandMetadata(userX), testCluster, compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyGetAll(commandMetadata(userY), testCluster, compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyGetAll(commandMetadata(userZ), testCluster, compact = false)) should beInvalid

    validateCommand(ss, Command.DeployedTopologyGetAll(commandMetadata(root), prodCluster, compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyGetAll(commandMetadata(userX), prodCluster, compact = false)) should beInvalid
    validateCommand(ss, Command.DeployedTopologyGetAll(commandMetadata(userY), prodCluster, compact = false)) should beInvalid
    validateCommand(ss, Command.DeployedTopologyGetAll(commandMetadata(userZ), prodCluster, compact = false)) should beInvalid
  }

  "DeployedTopologyMetricsGetAll" should "only be possible for super-users and users with READ kafka-cluster permission" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))
    ss.deployment.applyEntityStateChange(deploymentNewVersion(userX, Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userY, testCluster, PermissionResourceOperation.Read)))

    validateCommand(ss, Command.DeployedTopologyMetricsGetAll(commandMetadata(root), testCluster, compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyMetricsGetAll(commandMetadata(userX), testCluster, compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyMetricsGetAll(commandMetadata(userY), testCluster, compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyMetricsGetAll(commandMetadata(userZ), testCluster, compact = false)) should beInvalid

    validateCommand(ss, Command.DeployedTopologyMetricsGetAll(commandMetadata(root), prodCluster, compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyMetricsGetAll(commandMetadata(userX), prodCluster, compact = false)) should beInvalid
    validateCommand(ss, Command.DeployedTopologyMetricsGetAll(commandMetadata(userY), prodCluster, compact = false)) should beInvalid
    validateCommand(ss, Command.DeployedTopologyMetricsGetAll(commandMetadata(userZ), prodCluster, compact = false)) should beInvalid
  }

  "DeployedTopologyGet" should "only be possible for super-users and users with READ kafka-cluster and READ namespace permissions" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))
    ss.deployment.applyEntityStateChange(deploymentNewVersion(userX, Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, topologyPermission(userV, TopologyEntityId("test"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userV, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))

    validateCommand(ss, Command.DeployedTopologyGet(commandMetadata(root), testCluster, testTopologyId, compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyGet(commandMetadata(userX), testCluster, testTopologyId, compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyGet(commandMetadata(userY), testCluster, testTopologyId, compact = false)) should beInvalid
    validateCommand(ss, Command.DeployedTopologyGet(commandMetadata(userZ), testCluster, testTopologyId, compact = false)) should beInvalid
    validateCommand(ss, Command.DeployedTopologyGet(commandMetadata(userV), testCluster, testTopologyId, compact = false)) should beValid
  }

  "DeployedTopologyMetricsGet" should "only be possible for super-users and users with READ kafka-cluster and READ namespace permissions" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))
    ss.deployment.applyEntityStateChange(deploymentNewVersion(userX, Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, topologyPermission(userV, TopologyEntityId("test"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userV, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))

    validateCommand(ss, Command.DeployedTopologyMetricsGet(commandMetadata(root), testCluster, testTopologyId, compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyMetricsGet(commandMetadata(userX), testCluster, testTopologyId, compact = false)) should beValid
    validateCommand(ss, Command.DeployedTopologyMetricsGet(commandMetadata(userY), testCluster, testTopologyId, compact = false)) should beInvalid
    validateCommand(ss, Command.DeployedTopologyMetricsGet(commandMetadata(userZ), testCluster, testTopologyId, compact = false)) should beInvalid
    validateCommand(ss, Command.DeployedTopologyMetricsGet(commandMetadata(userV), testCluster, testTopologyId, compact = false)) should beValid
  }

  "DeployedTopologiesGetResolved" should "only be possible for super-users and users with READ kafka-cluster permission" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))
    ss.deployment.applyEntityStateChange(deploymentNewVersion(userX, Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Read)))

    validateCommand(ss, Command.DeployedTopologiesGetResolved(commandMetadata(root), testCluster, Seq(FlexibleName.Any()))) should beValid
    validateCommand(ss, Command.DeployedTopologiesGetResolved(commandMetadata(userX), testCluster, Seq(FlexibleName.Any()))) should beValid
    validateCommand(ss, Command.DeployedTopologiesGetResolved(commandMetadata(userY), testCluster, Seq(FlexibleName.Any()))) should beInvalid
    validateCommand(ss, Command.DeployedTopologiesGetResolved(commandMetadata(userZ), testCluster, Seq(FlexibleName.Any()))) should beValid
  }

  "DeployedTopologyReset" should "only be possible for super-users and users with DEPLOY kafka-cluster and RESET-APPLICATION namespace permission" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))
    ss.deployment.applyEntityStateChange(deploymentNewVersion(userX, Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userY, Namespace("test"), PermissionResourceOperation.ResetApplication)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userW, Namespace("test"), PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userW, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))

    validateCommand(ss, Command.DeployedTopologyReset(commandMetadata(root), testCluster, testTopologyId, DeployedTopology.ResetOptions(localAppId, resetOptions))) should beValid
    validateCommand(ss, Command.DeployedTopologyReset(commandMetadata(userX), testCluster, testTopologyId, DeployedTopology.ResetOptions(localAppId, resetOptions))) should beValid
    validateCommand(ss, Command.DeployedTopologyReset(commandMetadata(userY), testCluster, testTopologyId, DeployedTopology.ResetOptions(localAppId, resetOptions))) should beInvalid
    validateCommand(ss, Command.DeployedTopologyReset(commandMetadata(userZ), testCluster, testTopologyId, DeployedTopology.ResetOptions(localAppId, resetOptions))) should beInvalid
    validateCommand(ss, Command.DeployedTopologyReset(commandMetadata(userW), testCluster, testTopologyId, DeployedTopology.ResetOptions(localAppId, resetOptions))) should beInvalid
  }

  "DeployedTopology other get commands" should "only be possible for super-users and users with READ kafka-cluster and READ namespace permissions" in {
    val ss = AllStateEntities.InMemoryVersionedStateStores()
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(testCluster, "test-cluster-brokers")))
    ss.kafkaCluster.applyEntityStateChange(kafkaClusterNewVersion(root, KafkaCluster(prodCluster, "prod-cluster-brokers")))
    ss.topology.applyEntityStateChange(topologyNewVersion(userX, Topology(Namespace("test"))))
    ss.deployment.applyEntityStateChange(deploymentNewVersion(userX, Deployment(testCluster, testTopologyId, DeploymentTopologyVersion.Exact(1))))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userX, Namespace("test"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userX, testCluster, PermissionResourceOperation.Read)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userY, Namespace("test"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userZ, testCluster, PermissionResourceOperation.Read)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, topologyPermission(userV, TopologyEntityId("test"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userV, testCluster, PermissionResourceOperation.Read)))

    validateCommand(ss, Command.DeployedTopologyTopicGetAll(commandMetadata(root), testCluster, testTopologyId)) should beValid
    validateCommand(ss, Command.DeployedTopologyTopicGetAll(commandMetadata(userX), testCluster, testTopologyId)) should beValid
    validateCommand(ss, Command.DeployedTopologyTopicGetAll(commandMetadata(userY), testCluster, testTopologyId)) should beInvalid
    validateCommand(ss, Command.DeployedTopologyTopicGetAll(commandMetadata(userZ), testCluster, testTopologyId)) should beInvalid
    validateCommand(ss, Command.DeployedTopologyTopicGetAll(commandMetadata(userV), testCluster, testTopologyId)) should beValid

    validateCommand(ss, Command.DeployedTopologyTopicGet(commandMetadata(root), testCluster, testTopologyId, localTopicId)) should beValid
    validateCommand(ss, Command.DeployedTopologyTopicGet(commandMetadata(userX), testCluster, testTopologyId, localTopicId)) should beValid
    validateCommand(ss, Command.DeployedTopologyTopicGet(commandMetadata(userY), testCluster, testTopologyId, localTopicId)) should beInvalid
    validateCommand(ss, Command.DeployedTopologyTopicGet(commandMetadata(userZ), testCluster, testTopologyId, localTopicId)) should beInvalid
    validateCommand(ss, Command.DeployedTopologyTopicGet(commandMetadata(userV), testCluster, testTopologyId, localTopicId)) should beValid

    validateCommand(ss, Command.DeployedTopologyApplicationGetAll(commandMetadata(root), testCluster, testTopologyId)) should beValid
    validateCommand(ss, Command.DeployedTopologyApplicationGetAll(commandMetadata(userX), testCluster, testTopologyId)) should beValid
    validateCommand(ss, Command.DeployedTopologyApplicationGetAll(commandMetadata(userY), testCluster, testTopologyId)) should beInvalid
    validateCommand(ss, Command.DeployedTopologyApplicationGetAll(commandMetadata(userZ), testCluster, testTopologyId)) should beInvalid
    validateCommand(ss, Command.DeployedTopologyApplicationGetAll(commandMetadata(userV), testCluster, testTopologyId)) should beValid

    validateCommand(ss, Command.DeployedTopologyApplicationGet(commandMetadata(root), testCluster, testTopologyId, localAppId)) should beValid
    validateCommand(ss, Command.DeployedTopologyApplicationGet(commandMetadata(userX), testCluster, testTopologyId, localAppId)) should beValid
    validateCommand(ss, Command.DeployedTopologyApplicationGet(commandMetadata(userY), testCluster, testTopologyId, localAppId)) should beInvalid
    validateCommand(ss, Command.DeployedTopologyApplicationGet(commandMetadata(userZ), testCluster, testTopologyId, localAppId)) should beInvalid
    validateCommand(ss, Command.DeployedTopologyApplicationGet(commandMetadata(userV), testCluster, testTopologyId, localAppId)) should beValid
  }
}
