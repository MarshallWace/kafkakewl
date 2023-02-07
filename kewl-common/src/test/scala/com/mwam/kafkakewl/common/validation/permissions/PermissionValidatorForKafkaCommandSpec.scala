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
import com.mwam.kafkakewl.domain.KafkaClusterCommand
import org.scalatest.{FlatSpec, Matchers}

class PermissionValidatorForKafkaCommandSpec extends FlatSpec
  with Matchers
  with ValidationResultMatchers
  with PermissionValidatorCommon {

  "DeployTopology" should "only be possible for super-users and users with DEPLOY kafka-cluster and DEPLOY namespace permissions" in {
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

    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployTopology(commandMetadata(root), testCluster, testTopologyId, 1, 1, TopologyToDeploy(Namespace("test")), "", DeploymentOptions())) should beValid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployTopology(commandMetadata(userX), testCluster, testTopologyId, 1, 1, TopologyToDeploy(Namespace("test")), "", DeploymentOptions())) should beValid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployTopology(commandMetadata(userY), testCluster, testTopologyId, 1, 1, TopologyToDeploy(Namespace("test")), "", DeploymentOptions())) should beInvalid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployTopology(commandMetadata(userZ), testCluster, testTopologyId, 1, 1, TopologyToDeploy(Namespace("test")), "", DeploymentOptions())) should beInvalid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployTopology(commandMetadata(userW), testCluster, testTopologyId, 1, 1, TopologyToDeploy(Namespace("test")), "", DeploymentOptions())) should beInvalid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployTopology(commandMetadata(userV), testCluster, testTopologyId, 1, 1, TopologyToDeploy(Namespace("test")), "", DeploymentOptions())) should beValid
  }

  "UndeployTopology" should "only be possible for super-users and users with DEPLOY kafka-cluster and DEPLOY namespace permissions" in {
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

    validateKafkaClusterCommand(ss, KafkaClusterCommand.UndeployTopology(commandMetadata(root), testCluster, testTopologyId, Some(1), Some(1), "", DeploymentOptions())) should beValid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.UndeployTopology(commandMetadata(userX), testCluster, testTopologyId, Some(1), Some(1), "", DeploymentOptions())) should beValid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.UndeployTopology(commandMetadata(userY), testCluster, testTopologyId, Some(1), Some(1), "", DeploymentOptions())) should beInvalid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.UndeployTopology(commandMetadata(userZ), testCluster, testTopologyId, Some(1), Some(1), "", DeploymentOptions())) should beInvalid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.UndeployTopology(commandMetadata(userW), testCluster, testTopologyId, Some(1), Some(1), "", DeploymentOptions())) should beInvalid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.UndeployTopology(commandMetadata(userV), testCluster, testTopologyId, Some(1), Some(1), "", DeploymentOptions())) should beValid
  }

  "DeployedTopologyReset" should "only be possible for super-users and users with DEPLOY kafka-cluster and RESET-APPLICATION namespace permissions" in {
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
    ss.permission.applyEntityStateChange(permissionNewVersion(root, topologyPermission(userV, TopologyEntityId("test"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userV, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))

    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyReset(commandMetadata(root), testCluster, testTopologyId, DeployedTopology.ResetOptions(localAppId, resetOptions))) should beValid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyReset(commandMetadata(userX), testCluster, testTopologyId, DeployedTopology.ResetOptions(localAppId, resetOptions))) should beValid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyReset(commandMetadata(userY), testCluster, testTopologyId, DeployedTopology.ResetOptions(localAppId, resetOptions))) should beInvalid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyReset(commandMetadata(userZ), testCluster, testTopologyId, DeployedTopology.ResetOptions(localAppId, resetOptions))) should beInvalid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyReset(commandMetadata(userW), testCluster, testTopologyId, DeployedTopology.ResetOptions(localAppId, resetOptions))) should beInvalid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyReset(commandMetadata(userV), testCluster, testTopologyId, DeployedTopology.ResetOptions(localAppId, resetOptions))) should beValid
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
    ss.permission.applyEntityStateChange(permissionNewVersion(root, namespacePermission(userW, Namespace("test"), PermissionResourceOperation.Read)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userW, testCluster, PermissionResourceOperation.Read, PermissionResourceOperation.Deploy)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, topologyPermission(userV, TopologyEntityId("test"), PermissionResourceOperation.Any)))
    ss.permission.applyEntityStateChange(permissionNewVersion(root, kafkaClusterPermission(userV, testCluster, PermissionResourceOperation.Read)))

    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyTopicGetAll(commandMetadata(root), testCluster, testTopologyId)) should beValid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyTopicGetAll(commandMetadata(userX), testCluster, testTopologyId)) should beValid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyTopicGetAll(commandMetadata(userY), testCluster, testTopologyId)) should beInvalid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyTopicGetAll(commandMetadata(userZ), testCluster, testTopologyId)) should beInvalid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyTopicGetAll(commandMetadata(userW), testCluster, testTopologyId)) should beValid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyTopicGetAll(commandMetadata(userV), testCluster, testTopologyId)) should beValid

    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyTopicGet(commandMetadata(root), testCluster, testTopologyId, localTopicId)) should beValid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyTopicGet(commandMetadata(userX), testCluster, testTopologyId, localTopicId)) should beValid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyTopicGet(commandMetadata(userY), testCluster, testTopologyId, localTopicId)) should beInvalid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyTopicGet(commandMetadata(userZ), testCluster, testTopologyId, localTopicId)) should beInvalid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyTopicGet(commandMetadata(userW), testCluster, testTopologyId, localTopicId)) should beValid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyTopicGet(commandMetadata(userV), testCluster, testTopologyId, localTopicId)) should beValid

    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyApplicationGetAll(commandMetadata(root), testCluster, testTopologyId)) should beValid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyApplicationGetAll(commandMetadata(userX), testCluster, testTopologyId)) should beValid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyApplicationGetAll(commandMetadata(userY), testCluster, testTopologyId)) should beInvalid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyApplicationGetAll(commandMetadata(userZ), testCluster, testTopologyId)) should beInvalid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyApplicationGetAll(commandMetadata(userW), testCluster, testTopologyId)) should beValid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyApplicationGetAll(commandMetadata(userV), testCluster, testTopologyId)) should beValid

    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyApplicationGet(commandMetadata(root), testCluster, testTopologyId, localAppId)) should beValid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyApplicationGet(commandMetadata(userX), testCluster, testTopologyId, localAppId)) should beValid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyApplicationGet(commandMetadata(userY), testCluster, testTopologyId, localAppId)) should beInvalid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyApplicationGet(commandMetadata(userZ), testCluster, testTopologyId, localAppId)) should beInvalid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyApplicationGet(commandMetadata(userW), testCluster, testTopologyId, localAppId)) should beValid
    validateKafkaClusterCommand(ss, KafkaClusterCommand.DeployedTopologyApplicationGet(commandMetadata(userV), testCluster, testTopologyId, localAppId)) should beValid
  }
}
