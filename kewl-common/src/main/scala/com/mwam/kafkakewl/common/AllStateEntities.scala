/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.domain.deploy.{Deployment, DeploymentStateChange}
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterStateChange}
import com.mwam.kafkakewl.domain.permission.{Permission, PermissionStateChange}
import com.mwam.kafkakewl.domain.topology.{Topology, TopologyStateChange}

object AllStateEntities {
  val name: String = "state-entities"

  /**
    * The following few case classes are the central points where new entities need to be added.
    */
  final case class PerEntityCtx[FPermission[_ <: Entity], FTopology[_ <: Entity], FKafkaCluster[_ <: Entity], FDeployment[_ <: Entity]](
    permission: FPermission[Permission],
    topology: FTopology[Topology],
    kafkaCluster: FKafkaCluster[KafkaCluster],
    deployment: FDeployment[Deployment]
  )
  final case class PerEntityId[FPermission, FTopology, FKafkaCluster, FDeployment](
    permission: FPermission,
    topology: FTopology,
    kafkaCluster: FKafkaCluster,
    deployment: FDeployment
  )
  type SameCtx[F[_ <: Entity]] = PerEntityCtx[F, F, F, F]
  type SameId[F] = PerEntityId[F, F, F, F]

  /**
    * Apply state-change results
    */
  type ApplyEntityStateChangeResults = SameCtx[ApplyStateChange.ApplyResults]
  object ApplyEntityStateChangeResults {
    def apply(
      permission: ApplyStateChange.ApplyResults[Permission] = IndexedSeq.empty,
      topology: ApplyStateChange.ApplyResults[Topology] = IndexedSeq.empty,
      kafkaCluster: ApplyStateChange.ApplyResults[KafkaCluster] = IndexedSeq.empty,
      deployment: ApplyStateChange.ApplyResults[Deployment] = IndexedSeq.empty
    ): ApplyEntityStateChangeResults = new ApplyEntityStateChangeResults(permission, topology, kafkaCluster, deployment)
  }

  /**
    * Writable state stores
    */
  type WritableStateStores = PerEntityId[
    WritableStateStore[Permission, PermissionStateChange.StateChange],
    WritableStateStore[Topology, TopologyStateChange.StateChange],
    WritableStateStore[KafkaCluster, KafkaClusterStateChange.StateChange],
    WritableStateStore[Deployment, DeploymentStateChange.StateChange]]

  object WritableStateStores {
    implicit class AllStateEntitiesWritableStateStoresExtensions(writableStateStores: WritableStateStores) {
      def applyEntityStateChange(changes: AllStateEntitiesStateChanges): ApplyEntityStateChangeResults = {
        new ApplyEntityStateChangeResults(
          changes.permission.map(writableStateStores.permission.applyEntityStateChange),
          changes.topology.map(writableStateStores.topology.applyEntityStateChange),
          changes.kafkaCluster.map(writableStateStores.kafkaCluster.applyEntityStateChange),
          changes.deployment.map(writableStateStores.deployment.applyEntityStateChange)
        )
      }
    }
  }

  /**
    * Readable state stores
    */
  type ReadableStateStores = SameCtx[ReadableStateStore]
  object ReadableStateStores {
    implicit class AllStateEntitiesReadableStateStoresExtensions(readableStateStores: ReadableStateStores) {
      def snapshot: ReadableStateStores = {
        new ReadableStateStores(
          readableStateStores.permission.readableSnapshot,
          readableStateStores.topology.readableSnapshot,
          readableStateStores.kafkaCluster.readableSnapshot,
          readableStateStores.deployment.readableSnapshot
        )
      }
    }
  }

  /**
    * Readable versioned state stores
    */
  type ReadableVersionedStateStores = SameCtx[ReadableVersionedStateStore]
  object ReadableVersionedStateStores {
    implicit class AllStateEntitiesReadableVersionedStateStoresExtensions(readableVersionedStateStores: ReadableVersionedStateStores) {
      def toReadable: ReadableStateStores = {
        new ReadableStateStores(
          readableVersionedStateStores.permission,
          readableVersionedStateStores.topology,
          readableVersionedStateStores.kafkaCluster,
          readableVersionedStateStores.deployment
        )
      }

      def snapshot: ReadableVersionedStateStores = {
        new ReadableVersionedStateStores(
          readableVersionedStateStores.permission.readableSnapshot,
          readableVersionedStateStores.topology.readableSnapshot,
          readableVersionedStateStores.kafkaCluster.readableSnapshot,
          readableVersionedStateStores.deployment.readableSnapshot
        )
      }

      def withUncommittedChanges(
        changes: AllStateEntitiesStateChanges
      )(implicit
        permissionApplyStateChange: ApplyStateChange[Permission, PermissionStateChange.StateChange],
        topologyApplyStateChange: ApplyStateChange[Topology, TopologyStateChange.StateChange],
        kafkaClusterApplyStateChange: ApplyStateChange[KafkaCluster, KafkaClusterStateChange.StateChange],
        deploymentApplyStateChange: ApplyStateChange[Deployment, DeploymentStateChange.StateChange]
      ): ReadableVersionedStateStores = {

        new ReadableVersionedStateStores(
          new ReadableVersionedStateStoreWithUncommittedChanges(readableVersionedStateStores.permission, changes.permission),
          new ReadableVersionedStateStoreWithUncommittedChanges(readableVersionedStateStores.topology, changes.topology),
          new ReadableVersionedStateStoreWithUncommittedChanges(readableVersionedStateStores.kafkaCluster, changes.kafkaCluster),
          new ReadableVersionedStateStoreWithUncommittedChanges(readableVersionedStateStores.deployment, changes.deployment)
        )
      }
    }
  }

  /**
    * In-memory state stores
    */
  type InMemoryStateStores = PerEntityId[
    InMemoryStateStore[Permission, PermissionStateChange.StateChange],
    InMemoryStateStore[Topology, TopologyStateChange.StateChange],
    InMemoryStateStore[KafkaCluster, KafkaClusterStateChange.StateChange],
    InMemoryStateStore[Deployment, DeploymentStateChange.StateChange]]

  object InMemoryStateStores {
    def apply(): InMemoryStateStores =
      new InMemoryStateStores(
        InMemoryStateStore.forPermissions(),
        InMemoryStateStore.forTopology(),
        InMemoryStateStore.forKafkaCluster(),
        InMemoryStateStore.forDeployment()
      )

    implicit class AllStateEntitiesInMemoryStateStoresExtensions(inMemoryStateStores: AllStateEntities.InMemoryStateStores) {
      def toWritable: WritableStateStores = {
        new WritableStateStores(
          inMemoryStateStores.permission,
          inMemoryStateStores.topology,
          inMemoryStateStores.kafkaCluster,
          inMemoryStateStores.deployment
        )
      }

      def toReadable: ReadableStateStores = {
        new ReadableStateStores(
          inMemoryStateStores.permission,
          inMemoryStateStores.topology,
          inMemoryStateStores.kafkaCluster,
          inMemoryStateStores.deployment
        )
      }
    }
  }

  /**
    * In-memory versioned state stores
    */
  type InMemoryVersionedStateStores = PerEntityId[
    InMemoryVersionedStateStore[Permission, PermissionStateChange.StateChange],
    InMemoryVersionedStateStore[Topology, TopologyStateChange.StateChange],
    InMemoryVersionedStateStore[KafkaCluster, KafkaClusterStateChange.StateChange],
    InMemoryVersionedStateStore[Deployment, DeploymentStateChange.StateChange]]

  object InMemoryVersionedStateStores {
    def apply(): InMemoryVersionedStateStores =
      new InMemoryVersionedStateStores(
        InMemoryVersionedStateStore.forPermission(),
        InMemoryVersionedStateStore.forTopology(),
        InMemoryVersionedStateStore.forKafkaCluster(),
        InMemoryVersionedStateStore.forDeployment()
      )

    implicit class AllStateEntitiesInMemoryVersionedStateStoresExtensions(inMemoryVersionedStateStores: InMemoryVersionedStateStores) {
      def toWritable: WritableStateStores = {
        new WritableStateStores(
          inMemoryVersionedStateStores.permission,
          inMemoryVersionedStateStores.topology,
          inMemoryVersionedStateStores.kafkaCluster,
          inMemoryVersionedStateStores.deployment
        )
      }

      def toReadable: ReadableStateStores = {
        new ReadableStateStores(
          inMemoryVersionedStateStores.permission,
          inMemoryVersionedStateStores.topology,
          inMemoryVersionedStateStores.kafkaCluster,
          inMemoryVersionedStateStores.deployment
        )
      }

      def toReadableVersioned: ReadableVersionedStateStores = {
        new ReadableVersionedStateStores(
          inMemoryVersionedStateStores.permission,
          inMemoryVersionedStateStores.topology,
          inMemoryVersionedStateStores.kafkaCluster,
          inMemoryVersionedStateStores.deployment
        )
      }
    }
  }
}
