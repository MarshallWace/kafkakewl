/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.state

import java.util.UUID

import scala.reflect.runtime.universe._
import com.mwam.kafkakewl.common.{AllStateEntities, ReadableStateStore, ReadableVersionedStateStore}
import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.domain.Entity._
import StateCommandProcessingResult.ProcessFuncResult
import com.mwam.kafkakewl.domain.EntityCompactors._
import com.mwam.kafkakewl.domain.deploy.{Deployment, DeploymentStateChange}
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterStateChange}
import com.mwam.kafkakewl.domain.permission.{Permission, PermissionEntityId, PermissionStateChange}
import com.mwam.kafkakewl.domain.topology.{Topology, TopologyStateChange}

private[state] trait StateCommandProcessing {
  private def permissionsEquivalent(p1: Permission, p2: Permission): Boolean = p1 equivalent p2

  private def getAll[E <: Entity : TypeTag](
    command: Command,
    stateStore: ReadableStateStore[E],
    compact: Boolean
  )(
    implicit compactor: EntityCompactor[E]
  ): ProcessFuncResult = {
    ProcessFuncResult.success(command.succeededResult(stateStore.getLatestLiveStates, compact))
  }

  private def getById[E <: Entity : TypeTag](
    command: Command,
    stateStore: ReadableVersionedStateStore[E],
    id: EntityId,
    compact: Boolean,
    version: Option[Int] = None
  )(
    implicit compactor: EntityCompactor[E]
  ): ProcessFuncResult = {
    val entityStateOrNone = version
      .map(stateStore.getStateVersion(id, _))
      .getOrElse(stateStore.getLatestLiveState(id))

    entityStateOrNone match {
      case Some(state: EntityState.Live[E]) => ProcessFuncResult.success(command.succeededResult(state, compact))
      case _ => ProcessFuncResult.failure(
        version
          .map(v => command.validationFailedResult(s"${typeOf[E].entityName} $id/$v does not exists"))
          .getOrElse(command.validationFailedResult(s"${typeOf[E].entityName} $id does not exists"))
      )
    }
  }

  private def createOrUpdate[E <: Entity : TypeTag](
    command: Command,
    stateStore: ReadableStateStore[E],
    id: EntityId,
    newEntity: E,
    allStateChanges: EntityStateMetadata => AllStateEntitiesStateChanges
  ): ProcessFuncResult = {
    val previousIsSameAsNew = stateStore.getLatestLiveState(id).exists(_.entity == newEntity)
    ProcessFuncResult.success(
      command.succeededResult(entityStateChanged = !previousIsSameAsNew),
      // if the previous version is exactly the same as the new one -> generate no state change, just return success
      if (previousIsSameAsNew) AllStateEntitiesStateChanges()
      else allStateChanges(EntityStateMetadata(id.id, stateStore.getNextStateVersion(id), command.userName))
    )
  }

  private def createOrUpdateWithContent[E <: Entity : TypeTag](
    command: Command,
    stateStore: ReadableStateStore[E],
    newEntity: E,
    createNewEntityId: String => EntityId,
    entityEquals: (E, E) => Boolean,
    allStateChanges: EntityStateMetadata => AllStateEntitiesStateChanges
  ): ProcessFuncResult = {
    val id = stateStore.getLatestLiveStates.find(s => entityEquals(s.entity, newEntity)) match {
      case Some(matchingEntity) => createNewEntityId(matchingEntity.id)
      case None =>
        // no matching entity, insert a new one with a generated id
        createNewEntityId(UUID.randomUUID().toString.replace("-", ""))
    }
    createOrUpdate(
      command,
      stateStore,
      id,
      newEntity,
      allStateChanges
    )
  }

  private def delete[E <: Entity : TypeTag](
    command: Command,
    stateStore: ReadableStateStore[E],
    id: EntityId,
    allStateChanges: EntityStateMetadata => AllStateEntitiesStateChanges

  ): ProcessFuncResult =
    stateStore.getLatestLiveState(id) match {
      case Some(_) =>
        ProcessFuncResult.success(
          command.succeededResult(entityStateChanged = true),
          allStateChanges(EntityStateMetadata(id.id, stateStore.getNextStateVersion(id), command.userName))
        )
      case None => ProcessFuncResult.failure(command.validationFailedResult(s"${typeOf[E].entityName} $id does not exist"))
    }

  private def deleteByEntity[E <: Entity : TypeTag](
    command: Command,
    stateStore: ReadableStateStore[E],
    entity: E,
    entityEquals: (E, E) => Boolean,
    compact: Boolean,
    allStateChanges: IndexedSeq[EntityStateMetadata] => AllStateEntitiesStateChanges
  )(
    implicit compactor: EntityCompactor[E]
  ): ProcessFuncResult = {
    val matchingEntities = stateStore.getLatestLiveStates.filter(s => entityEquals(s.entity, entity))
    if (matchingEntities.isEmpty)
      ProcessFuncResult.failure(command.validationFailedResult(s"${typeOf[Permission].entityName} $entity does not exist"))
    else
      ProcessFuncResult.success(
        command.succeededResult(matchingEntities, compact),
        allStateChanges(matchingEntities.map(s => EntityStateMetadata(s.id, stateStore.getNextStateVersion(s.id), command.userName)))
      )
  }

  /**
    * Processes the specified command using the readonly state-store (it doesn't modify the state-store, only returns the necessary state-changes and the result).
    *
    * @param command the command to process
    * @param ss the readonly state-stores
    * @return the result of the command processing, the resulting state changes and the resulting deploy-commands to process.
    */
  def process(
    command: Command,
    ss: AllStateEntities.ReadableVersionedStateStores
  ): ProcessFuncResult = {
    command match {
      // get-all
      case c: Command.PermissionGetAll => getAll[Permission](c, ss.permission, c.compact)
      case c: Command.TopologyGetAll => getAll[Topology](c, ss.topology, c.compact)
      case c: Command.KafkaClusterGetAll => getAll[KafkaCluster](c, ss.kafkaCluster, c.compact)
      case c: Command.DeploymentGetAll => getAll[Deployment](c, ss.deployment, c.compact)

      // get-by-id
      case c: Command.PermissionGet => getById(c, ss.permission, c.permissionId, c.compact)
      case c: Command.TopologyGet => getById(c, ss.topology, c.topologyId, c.compact, c.version)
      case c: Command.KafkaClusterGet => getById(c, ss.kafkaCluster, c.kafkaClusterId, c.compact)
      case c: Command.DeploymentGet => getById(c, ss.deployment, c.deploymentId, c.compact)

      // create new entities
      case c: Command.PermissionCreate =>
        createOrUpdate(c, ss.permission, c.permissionId, c.permission, md => AllStateEntitiesStateChanges(PermissionStateChange.NewVersion(md, c.permission)))
      case c: Command.PermissionCreateWithContent =>
        createOrUpdateWithContent(c, ss.permission, c.permission, PermissionEntityId, permissionsEquivalent, md => AllStateEntitiesStateChanges(PermissionStateChange.NewVersion(md, c.permission)))
      case c: Command.TopologyCreate =>
        createOrUpdate(c, ss.topology, c.topologyId, c.topology, md => AllStateEntitiesStateChanges(TopologyStateChange.NewVersion(md, c.topology)))
      case c: Command.KafkaClusterCreate =>
        createOrUpdate(c, ss.kafkaCluster, c.kafkaClusterId, c.kafkaCluster, md => AllStateEntitiesStateChanges(KafkaClusterStateChange.NewVersion(md, c.kafkaCluster)))
      case c: Command.DeploymentCreate =>
        val deployment = DeploymentUtils.resolveDeploymentLatestOnce(ss.topology, c.deployment)
        createOrUpdate(c, ss.deployment, c.deploymentId, deployment, md => AllStateEntitiesStateChanges(DeploymentStateChange.NewVersion(md, deployment)))

      // update existing entities
      case c: Command.PermissionUpdate =>
        createOrUpdate(c, ss.permission, c.permissionId, c.permission, md => AllStateEntitiesStateChanges(PermissionStateChange.NewVersion(md, c.permission)))
      case c: Command.TopologyUpdate =>
        createOrUpdate(c, ss.topology, c.topologyId, c.topology, md => AllStateEntitiesStateChanges(TopologyStateChange.NewVersion(md, c.topology)))
      case c: Command.KafkaClusterUpdate =>
        createOrUpdate(c, ss.kafkaCluster, c.kafkaClusterId, c.kafkaCluster, md => AllStateEntitiesStateChanges(KafkaClusterStateChange.NewVersion(md, c.kafkaCluster)))
      case c: Command.DeploymentUpdate =>
        val deployment = DeploymentUtils.resolveDeploymentLatestOnce(ss.topology, c.deployment)
        createOrUpdate(c, ss.deployment, c.deploymentId, deployment, md => AllStateEntitiesStateChanges(DeploymentStateChange.NewVersion(md, deployment)))

      // delete existing entities
      case c: Command.PermissionDelete =>
        delete(c, ss.permission, c.permissionId, md => AllStateEntitiesStateChanges(PermissionStateChange.Deleted(md)))
      case c: Command.PermissionDeleteByContent =>
        deleteByEntity[Permission](c, ss.permission, c.permission, permissionsEquivalent, compact = false, mds => AllStateEntitiesStateChanges(mds.map(PermissionStateChange.Deleted)))
      case c: Command.TopologyDelete =>
        delete(c, ss.topology, c.topologyId, md => AllStateEntitiesStateChanges(TopologyStateChange.Deleted(md)))
      case c: Command.KafkaClusterDelete =>
        delete(c, ss.kafkaCluster, c.kafkaClusterId, md => AllStateEntitiesStateChanges(KafkaClusterStateChange.Deleted(md)))
      case c: Command.DeploymentDelete =>
        delete(c, ss.deployment, c.deploymentId, md => AllStateEntitiesStateChanges(DeploymentStateChange.Deleted(md)))

      // other state commands - no need to do anything here, just succeed (the kafka-cluster command processing logic may generate kafka-commands for these)
      case c: Command.StateCommand =>
        ProcessFuncResult.success(c.succeededResult())

      case c => ProcessFuncResult.failure(c.validationFailedResult(s"cannot process $c"))
    }
  }
}
