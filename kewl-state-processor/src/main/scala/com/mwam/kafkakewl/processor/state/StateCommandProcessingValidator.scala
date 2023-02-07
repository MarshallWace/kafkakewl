/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.state

import com.mwam.kafkakewl.common.AllStateEntities
import com.mwam.kafkakewl.common.validation.Validation._
import com.mwam.kafkakewl.common.validation._
import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.common.AllStateEntities.ReadableVersionedStateStores._
import com.mwam.kafkakewl.domain.deploy.{Deployment, DeploymentChange, DeploymentEntityId}
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.permission.{Permission, PermissionEntityId}
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology.{Topology, TopologyEntityId}

private[state] object StateCommandProcessingValidator extends ValidationUtils {
  type ValidatorFunc = (Command, AllStateEntities.ReadableVersionedStateStores) => Validation.Result

  def validateTopologyDelete(
    ss: AllStateEntities.ReadableStateStores,
    topologyId: TopologyEntityId
  ): Validation.Result = {
    val deploymentKafkaClusterIds = ss.deployment.getLatestLiveStates
      .filter(_.entity.topologyId == topologyId)
      .map(_.entity.kafkaClusterId.quote)
    Validation.Result.validationErrorIf(
      deploymentKafkaClusterIds.nonEmpty,
      s"cannot delete topology ${topologyId.quote} because there are deployments in ${deploymentKafkaClusterIds.mkString(", ")} kafka-cluster(s)"
    )
  }

  def validateTopology(
    ss: AllStateEntities.ReadableStateStores,
    topologyId: TopologyEntityId,
    topologyOrNone: Option[Topology],
    topicDefaults: TopicDefaults
  ): Validation.Result = {
    val deleteResult = if (topologyOrNone.isEmpty) validateTopologyDelete(ss, topologyId) else Validation.Result.success

    val currentTopologies = ss.topology.getLatestLiveStates.map(s => (TopologyEntityId(s.id), s.entity)).toMap
    val validationResult = TopologyValidator.validateTopology(currentTopologies, topologyId, topologyOrNone, topicDefaults)

    validationResult ++ deleteResult
  }

  def validateKafkaCluster(
    kafkaClusterCommandProcessorJaasConfig: Option[String],
    ss: AllStateEntities.ReadableStateStores,
    kafkaClusterId: KafkaClusterEntityId,
    kafkaClusterOrNone: Option[KafkaCluster]
  ): Validation.Result = {
    val currentKafkaClusters = ss.kafkaCluster.getLatestLiveStates.map(s => (KafkaClusterEntityId(s.id), s.entity)).toMap
    Seq(
      kafkaClusterOrNone
        .map(KafkaClusterValidator.validateKafkaCluster(kafkaClusterCommandProcessorJaasConfig, kafkaClusterId, _))
        .getOrElse(Validation.Result.success),
      KafkaClusterValidator.validateKafkaClusterWithOthers(currentKafkaClusters, kafkaClusterId, kafkaClusterOrNone)
    ).combine()
  }

  def validateKafkaClusterUpdate(ss: AllStateEntities.ReadableStateStores, id: KafkaClusterEntityId, kafkaCluster: KafkaCluster): Validation.Result = {
    val updateResultOrNone = for {
      existingKafkaClusterState <- ss.kafkaCluster.getLatestLiveState(id)
      existingKafkaCluster = existingKafkaClusterState.entity
    } yield {
      val brokersResult = if (existingKafkaCluster.brokers != kafkaCluster.brokers)
        Validation.Result.validationError(s"cannot change an existing kafka-cluster's brokers (${existingKafkaCluster.brokers} != ${kafkaCluster.brokers})")
      else
        Validation.Result.success

      // TODO updating the securityProtocol should be possible (when the cluster starts being a PLAINTEXT one, and later the admin sets up authentication)
      val securityProtocolResult = if (existingKafkaCluster.securityProtocol != kafkaCluster.securityProtocol)
        Validation.Result.validationError(s"cannot change an existing kafka-cluster's securityProtocol (${existingKafkaCluster.securityProtocol} != ${kafkaCluster.securityProtocol})")
      else
        Validation.Result.success

      brokersResult ++ securityProtocolResult
    }
    updateResultOrNone.getOrElse(Validation.Result.success)
  }

  def validateKafkaClusterDelete(ss: AllStateEntities.ReadableStateStores, id: KafkaClusterEntityId): Validation.Result = {
    val deploymentTopologyIdsOfKafkaCluster = ss.deployment.getLatestLiveStates.filter(_.entity.kafkaClusterId == id).map(_.entity.topologyId)
    if (deploymentTopologyIdsOfKafkaCluster.nonEmpty)
      Validation.Result.validationError(s"cannot delete kafka-cluster $id, because it's being used by deployments of topologies ${deploymentTopologyIdsOfKafkaCluster.mkString(", ")}")
    else
      Validation.Result.success
  }

  def ensureNoEquivalentPermissions(
    ss: AllStateEntities.ReadableStateStores,
    permissionId: PermissionEntityId,
    permission: Permission
  ): Validation.Result = {
    val currentPermissions = ss.permission.getLatestLiveStates.filter(_.id != permissionId.id)
    val existingEquivalentPermissions = currentPermissions.filter(_.entity.equivalent(permission)).map(_.id)
    Validation.Result.validationErrorIf(existingEquivalentPermissions.nonEmpty, s"permissions ${existingEquivalentPermissions.mkString(", ")} are equivalent to this one")
  }

  def validatePermission(
    ss: AllStateEntities.ReadableStateStores,
    permissionId: Option[PermissionEntityId],
    permission: Permission
  ): Validation.Result = {
    val equivalentValidationResult = permissionId.map(ensureNoEquivalentPermissions(ss, _, permission)).getOrElse(Validation.Result.success)
    // no other validation for now
    equivalentValidationResult
  }

  def validateDeployment(ss: AllStateEntities.ReadableStateStores, id: DeploymentEntityId, deployment: DeploymentChange): Validation.Result = {
    val (kafkaClusterId, topologyId) = Deployment.parseId(id)

    val kafkaClusterResult = Validation.Result.validationErrorIf(
      kafkaClusterId != deployment.kafkaClusterId,
      s"the deployment's kafkaClusterId (${deployment.kafkaClusterId}) is expected to be the same as the one in id ($kafkaClusterId)"
    )
    val topologyResult = Validation.Result.validationErrorIf(
      topologyId != deployment.topologyId,
      s"the deployment's topologyId (${deployment.topologyId}) is expected to be the same as the one in id ($topologyId)"
    )
    val kafkaClusterExistsResult = Validation.Result.validationErrorIf(
      ss.kafkaCluster.getLatestLiveState(kafkaClusterId).isEmpty,
      s"no live kafka-cluster '$kafkaClusterId'"
    )
    val topologyExistsResult = Validation.Result.validationErrorIf(
      ss.topology.getLatestLiveState(topologyId).isEmpty,
      s"no live topology '$topologyId'"
    )

    kafkaClusterResult ++ topologyResult ++ kafkaClusterExistsResult ++ topologyExistsResult
  }

  def validateTopologyId(topologyId: TopologyEntityId): Result =
    topologyId.id.validateRegexWholeString("[^/]+", "topology id cannot be empty and cannot contain '/'")

  def validateKafkaClusterId(kafkaClusterId: KafkaClusterEntityId): Result =
    kafkaClusterId.id.validateRegexWholeString("[^/]+", "kafka cluster id cannot be empty and cannot contain '/'")

  def validateCommand(kafkaClusterCommandProcessorJaasConfig: Option[String], topicDefaults: TopicDefaults) : ValidatorFunc =
    (command: Command, ss: AllStateEntities.ReadableVersionedStateStores) =>
      command match {
        case c: Command.TopologyCreate =>
          validateTopologyId(c.topologyId) ++ validateTopology(ss.toReadable, c.topologyId, Some(c.topology), topicDefaults)
        case c: Command.TopologyUpdate =>
          validateTopology(ss.toReadable, c.topologyId, Some(c.topology), topicDefaults)
        case c: Command.TopologyDelete =>
          EntityValidator.validateExists(ss.topology, c.topologyId) ++ validateTopology(ss.toReadable, c.topologyId, None, topicDefaults)

        case c: Command.KafkaClusterCreate =>
          validateKafkaClusterId(c.kafkaClusterId) ++
            validateKafkaCluster(kafkaClusterCommandProcessorJaasConfig, ss.toReadable, c.kafkaClusterId, Some(c.kafkaCluster))
        case c: Command.KafkaClusterUpdate =>
          validateKafkaCluster(kafkaClusterCommandProcessorJaasConfig, ss.toReadable, c.kafkaClusterId, Some(c.kafkaCluster)) ++
            validateKafkaClusterUpdate(ss.toReadable, c.kafkaClusterId, c.kafkaCluster)
        case c: Command.KafkaClusterDelete =>
          EntityValidator.validateExists(ss.kafkaCluster, c.kafkaClusterId) ++
            validateKafkaCluster(kafkaClusterCommandProcessorJaasConfig, ss.toReadable, c.kafkaClusterId, None) ++
            validateKafkaClusterDelete(ss.toReadable, c.kafkaClusterId)

        case c: Command.PermissionCreate =>
          validatePermission(ss.toReadable, Some(c.permissionId), c.permission)
        case c: Command.PermissionCreateWithContent =>
          validatePermission(ss.toReadable, None, c.permission)
        case c: Command.PermissionUpdate =>
          validatePermission(ss.toReadable, Some(c.permissionId), c.permission)
        case c: Command.PermissionDelete =>
          EntityValidator.validateExists(ss.permission, c.permissionId)
        case c: Command.PermissionDeleteByContent =>
          EntityValidator.validateExists(ss.permission, c.permission) // no validation of the actual permission to support deleting "invalid" permissions

        case c: Command.DeploymentCreate =>
          validateDeployment(ss.toReadable, c.deploymentId, c.deployment)
        case c: Command.DeploymentUpdate =>
          validateDeployment(ss.toReadable, c.deploymentId, c.deployment)
        case c: Command.DeploymentDelete =>
          EntityValidator.validateExists(ss.deployment, c.deploymentId)
        case c: Command.DeploymentReApply =>
          EntityValidator.validateExists(ss.deployment, c.deploymentId)

        case _ => Validation.Result.success
      }
}
