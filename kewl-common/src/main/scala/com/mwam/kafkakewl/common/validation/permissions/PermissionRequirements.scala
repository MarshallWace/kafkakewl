/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation.permissions

import com.mwam.kafkakewl.common.AllStateEntities
import com.mwam.kafkakewl.domain.Command.{AdminCommand, PermissionEntityCommand, PermissionPluginGetUserPermissions, PermissionPluginInvalidateCachedUserPermissions}
import com.mwam.kafkakewl.domain.deploy.{DeployedTopology, DeployedTopologyCompact, DeployedTopologyEntityId, Deployment}
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.metrics.{DeployedTopologyMetrics, DeployedTopologyMetricsCompact}
import com.mwam.kafkakewl.domain.permission.{Permission, PermissionResourceOperation}
import com.mwam.kafkakewl.domain.topology.{Topology, TopologyCompact, TopologyEntityId, TopologyToDeploy}
import com.mwam.kafkakewl.domain.{Command, Entity, KafkaClusterCommand}

/**
  * Finds out the required resource-operations for commands and entities.
  *
  * It needs the current state-stores too so that it can use other than the id of the entities to figure out the permission-requirements.
  *
  * @param stateStores the current state-stores
  */
final case class PermissionRequirements(stateStores: AllStateEntities.ReadableVersionedStateStores) {
  private def kafkaClusterOrNone(id: KafkaClusterEntityId): Option[KafkaCluster] =
    stateStores.kafkaCluster.getLatestLiveState(id).map(_.entity)

  private def topologyOrNone(id: TopologyEntityId, version: Option[Int] = None): Option[Topology] = version match {
    case Some(v) => stateStores.topology.getLiveStateVersion(id, v).map(_.entity)
    case None => stateStores.topology.getLatestLiveState(id).map(_.entity)
  }

  private def forStoredTopology(
    id: TopologyEntityId,
    version: Option[Int] = None,
    operation: PermissionResourceOperation = PermissionResourceOperation.Write
  ): Set[PermissionRequirement] =
    topologyOrNone(id, version).map(t => PermissionRequirement.topology(id, t.namespace, operation)).toSet

  private def forTopology(
    id: TopologyEntityId,
    topology: Topology,
    operation: PermissionResourceOperation = PermissionResourceOperation.Write
  ): Set[PermissionRequirement] =
    Set(PermissionRequirement.topology(id, topology.namespace, operation))

  private def forTopologyCompact(
    id: TopologyEntityId,
    topology: TopologyCompact,
    operation: PermissionResourceOperation = PermissionResourceOperation.Write
  ): Set[PermissionRequirement] =
    Set(PermissionRequirement.topology(id, topology.namespace, operation))

  private def forStoredKafkaCluster(
    id: KafkaClusterEntityId,
    operation: PermissionResourceOperation = PermissionResourceOperation.Write
  ): Set[PermissionRequirement] =
    kafkaClusterOrNone(id).map(PermissionRequirement.kafkaCluster(id, _, operation)).toSet

  private def forKafkaCluster(
    id: KafkaClusterEntityId,
    kafkaCluster: KafkaCluster,
    operation: PermissionResourceOperation = PermissionResourceOperation.Write
  ): Set[PermissionRequirement] =
    Set(PermissionRequirement.kafkaCluster(id, kafkaCluster, operation))

  private def forResolvedTopology(
    id: TopologyEntityId,
    topology: Topology.ResolvedTopology,
    operation: PermissionResourceOperation = PermissionResourceOperation.Write
  ): Set[PermissionRequirement] =
    Set(PermissionRequirement.topology(id, topology.namespace, operation))

  private def forResolvedTopologyToDeploy(
    id: DeployedTopologyEntityId,
    topology: TopologyToDeploy.ResolvedTopology,
    operation: PermissionResourceOperation = PermissionResourceOperation.Write
  ): Set[PermissionRequirement] =
    Set(PermissionRequirement.topology(topology.topologyEntityId, topology.namespace, operation))

  def forAdminCommand(command: AdminCommand): Set[PermissionRequirement] =
    command match {
      case _: PermissionPluginInvalidateCachedUserPermissions => Set(PermissionRequirement.systemRead)
      case _: PermissionPluginGetUserPermissions => Set(PermissionRequirement.systemRead)
      case _ => Set(PermissionRequirement.systemAny)
    }

  def forPermissionCommand(command: PermissionEntityCommand): Set[PermissionRequirement] = {
    command match {
      case _: Command.PermissionGetAll | _: Command.PermissionGet =>
        Set.empty
      case _: Command.PermissionCreate | _: Command.PermissionCreateWithContent | _: Command.PermissionUpdate | _: Command.PermissionDelete | _: Command.PermissionDeleteByContent =>
        Set(PermissionRequirement.systemAny)
    }
  }

  def forTopologyCommand(command: Command.TopologyEntityCommand): Set[PermissionRequirement] = {
    command match {
      case _: Command.TopologyGetAll => Set.empty // filtering the results for kafka-cluster READ permission
      case c: Command.TopologyGet => forStoredTopology(c.topologyId, operation = PermissionResourceOperation.Read)
      case c: Command.TopologyCreate => forStoredTopology(c.topologyId) ++ forTopology(c.topologyId, c.topology)
      case c: Command.TopologyUpdate => forStoredTopology(c.topologyId) ++ forTopology(c.topologyId, c.topology)
      case c: Command.TopologyDelete => forStoredTopology(c.topologyId)
    }
  }

  def forKafkaClusterCommand(command: Command.KafkaClusterEntityCommand): Set[PermissionRequirement] = {
    command match {
      case _: Command.KafkaClusterGetAll => Set.empty // filtering the results for kafka-cluster READ permission
      case c: Command.KafkaClusterGet => forStoredKafkaCluster(c.kafkaClusterId, PermissionResourceOperation.Read)
      case c: Command.KafkaClusterCreate => forStoredKafkaCluster(c.kafkaClusterId) ++ forKafkaCluster(c.kafkaClusterId, c.kafkaCluster)
      case c: Command.KafkaClusterUpdate => forStoredKafkaCluster(c.kafkaClusterId) ++ forKafkaCluster(c.kafkaClusterId, c.kafkaCluster)
      case c: Command.KafkaClusterDelete => forStoredKafkaCluster(c.kafkaClusterId)
    }
  }

  def forDeploymentEntityCommand(command: Command.DeploymentEntityCommand): Set[PermissionRequirement] = {
    // checking permissions only for the kafka-cluster
    // can't check the topology, because we don't know yet the exact version that'll be deployed
    // that will be checked on the kafka-cluster command
    command match {
      case _: Command.DeploymentGetAll => Set.empty // filtering the results for kafka-cluster READ permission
      case c: Command.DeploymentGet =>
        forStoredKafkaCluster(c.kafkaClusterId, PermissionResourceOperation.Read) ++
          forStoredTopology(c.topologyId, operation = PermissionResourceOperation.Read)
      case c: Command.DeploymentCreate =>
        forStoredKafkaCluster(c.kafkaClusterId, PermissionResourceOperation.Deploy) ++
          forStoredTopology(c.topologyId, operation = PermissionResourceOperation.Deploy)
      case c: Command.DeploymentUpdate =>
        forStoredKafkaCluster(c.kafkaClusterId, PermissionResourceOperation.Deploy) ++
          forStoredTopology(c.topologyId, operation = PermissionResourceOperation.Deploy)
      case c: Command.DeploymentDelete =>
        forStoredKafkaCluster(c.kafkaClusterId, PermissionResourceOperation.Deploy) ++
          forStoredTopology(c.topologyId, operation = PermissionResourceOperation.Deploy)
      case c: Command.DeploymentReApply =>
        forStoredKafkaCluster(c.kafkaClusterId, PermissionResourceOperation.Deploy) ++
          forStoredTopology(c.topologyId, operation = PermissionResourceOperation.Deploy)
    }
  }

  def forKafkaClusterDeployedTopologyEntityCommand(command: Command.KafkaClusterDeployedTopologyEntityCommand): Set[PermissionRequirement] = {
    // all we can check here is the kafka-cluster permission, everything else will have to be done at the kafka cluster command processing
    command match {
      case c: Command.DeployedTopologyGetAll =>
        forStoredKafkaCluster(c.kafkaClusterId, PermissionResourceOperation.Read)
      case c: Command.DeployedTopologyMetricsGetAll =>
        forStoredKafkaCluster(c.kafkaClusterId, PermissionResourceOperation.Read)
      case c: Command.DeployedTopologyGet =>
        forStoredKafkaCluster(c.kafkaClusterId, PermissionResourceOperation.Read) ++
          forStoredTopology(c.topologyId, operation = PermissionResourceOperation.Read)
      case c: Command.DeployedTopologyMetricsGet =>
        forStoredKafkaCluster(c.kafkaClusterId, PermissionResourceOperation.Read) ++
          forStoredTopology(c.topologyId, operation = PermissionResourceOperation.Read)
      case c: Command.DeployedTopologyReset =>
        forStoredKafkaCluster(c.kafkaClusterId, PermissionResourceOperation.Deploy) ++
          forStoredTopology(c.topologyId, operation = PermissionResourceOperation.ResetApplication)
      case c: Command.DeployedTopologyTopicGetAll =>
        forStoredKafkaCluster(c.kafkaClusterId, PermissionResourceOperation.Read) ++
          forStoredTopology(c.topologyId, operation = PermissionResourceOperation.Read)
      case c: Command.DeployedTopologyTopicGet =>
        forStoredKafkaCluster(c.kafkaClusterId, PermissionResourceOperation.Read) ++
          forStoredTopology(c.topologyId, operation = PermissionResourceOperation.Read)
      case c: Command.DeployedTopologyApplicationGetAll =>
        forStoredKafkaCluster(c.kafkaClusterId, PermissionResourceOperation.Read) ++
          forStoredTopology(c.topologyId, operation = PermissionResourceOperation.Read)
      case c: Command.DeployedTopologyApplicationGet =>
        forStoredKafkaCluster(c.kafkaClusterId, PermissionResourceOperation.Read) ++
          forStoredTopology(c.topologyId, operation = PermissionResourceOperation.Read)
      case c: Command.DeployedTopologiesGetResolved =>
        forStoredKafkaCluster(c.kafkaClusterId, PermissionResourceOperation.Read)
    }
  }

  def forDeployedTopologyEntityCommand(command: Command.DeployedTopologyEntityCommand): Set[PermissionRequirement] = {
    command match {
      case c: Command.KafkaClusterDeployedTopologyEntityCommand => forKafkaClusterDeployedTopologyEntityCommand(c)
      case _: Command.DeployedTopologyGetKafkaClustersAll => Set.empty // filtering the results for kafka-cluster READ permission
      case _: Command.DeployedTopologyMetricsGetKafkaClustersAll => Set.empty // filtering the results for kafka-cluster READ permission
    }
  }

  def forCommand(command: Command): Set[PermissionRequirement] = command match {
    case c: Command.AdminCommand => forAdminCommand(c)
    case c: Command.PermissionEntityCommand => forPermissionCommand(c)
    case c: Command.TopologyEntityCommand => forTopologyCommand(c)
    case c: Command.KafkaClusterEntityCommand => forKafkaClusterCommand(c)
    case c: Command.DeploymentEntityCommand => forDeploymentEntityCommand(c)
    case c: Command.DeployedTopologyEntityCommand => forDeployedTopologyEntityCommand(c)
    case _: Command.HealthIsLive | _: Command.HealthIsReady => Set.empty
  }

  def forCommand(command: KafkaClusterCommand): Set[PermissionRequirement] = {
    command match {
      case c: KafkaClusterCommand.DeployTopology =>
        forStoredKafkaCluster(command.kafkaClusterId, PermissionResourceOperation.Deploy) ++
          // checking against that exact topology-version we're deploying
          forStoredTopology(c.topologyId, Some(c.topologyVersion), PermissionResourceOperation.Deploy)
      case c: KafkaClusterCommand.UndeployTopology =>
        forStoredKafkaCluster(command.kafkaClusterId, PermissionResourceOperation.Deploy) ++
          // we don't always know what topology version we're trying to un-deploy (e.g. when a removed deployment is re-applied),
          // in those case we just check deploy permission for the latest version of the topology
          forStoredTopology(c.topologyId, c.topologyVersion, PermissionResourceOperation.Deploy)
      case c: KafkaClusterCommand.DeployedTopologyReset =>
        forStoredKafkaCluster(command.kafkaClusterId, PermissionResourceOperation.Deploy) ++
          forStoredTopology(c.topologyId, operation = PermissionResourceOperation.ResetApplication)
      case c: KafkaClusterCommand.DeployedTopologyReadOperation =>
        forStoredKafkaCluster(command.kafkaClusterId, PermissionResourceOperation.Read) ++
          forStoredTopology(c.topologyId, operation = PermissionResourceOperation.Read)

      case _: KafkaClusterCommand.Diff => Set(PermissionRequirement.systemAny)

      // for other commands, no extra permission apart from the kafka-cluster deploy
      case _ => forStoredKafkaCluster(command.kafkaClusterId, PermissionResourceOperation.Deploy)
    }
  }

  def forEntity(id: String, entity: Entity): Set[PermissionRequirement] = entity match {
    case permission: Permission =>
      Set(PermissionRequirement.PrincipalOrSystemRead(permission.principal))

    case topology: Topology =>
      forTopology(TopologyEntityId(id), topology, operation = PermissionResourceOperation.Read)

    case topology: TopologyCompact =>
      forTopologyCompact(TopologyEntityId(id), topology, operation = PermissionResourceOperation.Read)

    case resolvedTopology: Topology.ResolvedTopology =>
      forResolvedTopology(TopologyEntityId(id), resolvedTopology, operation = PermissionResourceOperation.Read)

    case kafkaCluster: KafkaCluster =>
      forStoredKafkaCluster(kafkaCluster.kafkaCluster, operation = PermissionResourceOperation.Read)

    case deployment: Deployment =>
      forStoredKafkaCluster(deployment.kafkaClusterId, operation = PermissionResourceOperation.Read) ++
        forStoredTopology(deployment.topologyId, operation = PermissionResourceOperation.Read)

    case deployedTopology: DeployedTopology =>
      forStoredKafkaCluster(deployedTopology.kafkaClusterId, operation = PermissionResourceOperation.Read) ++
        forStoredTopology(deployedTopology.topologyId, operation = PermissionResourceOperation.Read)

    case deployedTopology: DeployedTopologyCompact =>
      forStoredKafkaCluster(deployedTopology.kafkaClusterId, operation = PermissionResourceOperation.Read) ++
        forStoredTopology(deployedTopology.topologyId, operation = PermissionResourceOperation.Read)

    case deployedTopologyMetrics: DeployedTopologyMetrics =>
      forStoredKafkaCluster(deployedTopologyMetrics.kafkaClusterId, operation = PermissionResourceOperation.Read) ++
        forStoredTopology(deployedTopologyMetrics.topologyId, operation = PermissionResourceOperation.Read)

    case deployedTopologyMetrics: DeployedTopologyMetricsCompact =>
      forStoredKafkaCluster(deployedTopologyMetrics.kafkaClusterId, operation = PermissionResourceOperation.Read) ++
        forStoredTopology(deployedTopologyMetrics.topologyId, operation = PermissionResourceOperation.Read)

    case resolvedTopology: TopologyToDeploy.ResolvedTopology =>
      forResolvedTopologyToDeploy(DeployedTopologyEntityId(id), resolvedTopology, operation = PermissionResourceOperation.Read)

    case _ =>
      Set.empty
  }
}
