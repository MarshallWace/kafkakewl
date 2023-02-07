/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation.permissions

import java.util.UUID

import com.mwam.kafkakewl.common.AllStateEntities.InMemoryVersionedStateStores._
import com.mwam.kafkakewl.common.validation.PermissionValidator
import com.mwam.kafkakewl.common.validation.Validation.Result
import com.mwam.kafkakewl.common.AllStateEntities
import com.mwam.kafkakewl.domain.deploy.DeployedTopology.ResetApplicationOptions
import com.mwam.kafkakewl.domain.deploy._
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterAndTopology, KafkaClusterEntityId, KafkaClusterStateChange}
import com.mwam.kafkakewl.domain.permission._
import com.mwam.kafkakewl.domain.topology._
import com.mwam.kafkakewl.domain.{Command, CommandMetadata, Entity, EntityCompactor, EntityStateMetadata, FlexibleName, KafkaClusterCommand}

trait PermissionValidatorCommon  {
  def permissionValidator(superUsers: Seq[String]) = new PermissionValidator(superUsers)
  def newId(): String = UUID.randomUUID().toString.replace("-", "")

  def commandMetadata(userName: String) = CommandMetadata(userName)

  def kafkaClusterPermission(principal: String, kafkaClusterId: KafkaClusterEntityId, operations: PermissionResourceOperation*) =
    Permission(principal, PermissionResourceType.KafkaCluster, FlexibleName.Exact(kafkaClusterId.id), operations.toSet)

  def namespacePermission(principal: String, namespace: Namespace, operations: PermissionResourceOperation*) =
    Permission(principal, PermissionResourceType.Namespace, FlexibleName.Namespace(namespace.ns), operations.toSet)

  def topologyPermission(principal: String, topologyId: TopologyEntityId, operations: PermissionResourceOperation*) =
    Permission(principal, PermissionResourceType.Topology, FlexibleName.Exact(topologyId.id), operations.toSet)

  def systemPermission(principal: String, operations: PermissionResourceOperation*) =
    Permission(principal, PermissionResourceType.System, FlexibleName.Any(), operations.toSet)

  def permissionNewVersion(user: String, permission: Permission, permissionId: PermissionEntityId = PermissionEntityId(newId()), version: Int = 1) =
    PermissionStateChange.NewVersion(EntityStateMetadata(permissionId.id, version, user), permission)

  def kafkaClusterNewVersion(user: String, kafkaCluster: KafkaCluster, version: Int = 1) =
    KafkaClusterStateChange.NewVersion(EntityStateMetadata(kafkaCluster.kafkaCluster.id, version, user), kafkaCluster)

  def topologyNewVersion(user: String, topology: Topology, version: Int = 1) =
    TopologyStateChange.NewVersion(EntityStateMetadata(topology.topologyEntityId.id, version, user), topology)

  def deploymentNewVersion(user: String, deployment: Deployment, version: Int = 1) =
    DeploymentStateChange.NewVersion(EntityStateMetadata(KafkaClusterAndTopology.id(deployment.kafkaClusterId, deployment.topologyId), version, user), deployment)

  val resetOptions = ResetApplicationOptions.ApplicationTopics()

  val testTopologyId = TopologyEntityId("test")
  val localAppId = LocalApplicationId("appId")
  val localTopicId = LocalTopicId("topicId")

  val testCluster = KafkaClusterEntityId("test-cluster")
  val prodCluster = KafkaClusterEntityId("prod-cluster")

  val root = "root"
  val userX = "userX"
  val userY = "userY"
  val userZ = "userZ"
  val userW = "userW"
  val userV = "userv"

  def validateCommand(
    stateStores: AllStateEntities.InMemoryVersionedStateStores,
    command: Command,
    superUsers: Seq[String] = Seq(root)
  ): Result = permissionValidator(superUsers).validateCommandPermissions(stateStores.toReadableVersioned, command)

  def validateKafkaClusterCommand(
    stateStores: AllStateEntities.InMemoryVersionedStateStores,
    command: KafkaClusterCommand,
    superUsers: Seq[String] = Seq(root)
  ): Result = permissionValidator(superUsers).validateCommandPermissions(stateStores.toReadableVersioned, command)

  def validateEntity(
    stateStores: AllStateEntities.InMemoryVersionedStateStores,
    userName: String,
    id: String,
    entity: Entity,
    superUsers: Seq[String] = Seq(root)
  ): Result = permissionValidator(superUsers).validateEntityPermissions(userName, stateStores.toReadableVersioned, id, entity)

  def compactAndValidateEntity[E <: Entity](
    stateStores: AllStateEntities.InMemoryVersionedStateStores,
    userName: String,
    id: String,
    entity: E,
    superUsers: Seq[String] = Seq(root)
  )(implicit compactor: EntityCompactor[E]): Result = validateEntity(stateStores, userName, id, compactor.compact(entity), superUsers)
}
