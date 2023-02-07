/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import com.mwam.kafkakewl.domain.deploy.{DeployedTopology, DeployedTopologyStateChange}
import com.mwam.kafkakewl.domain.{AllDeploymentEntitiesStateChanges, Entity}

object AllDeploymentEntities {
  val name: String = "deployment-entities"

  /**
    * The following few case classes are the central points where new entities need to be added.
    */
  final case class PerEntityCtx[FDeployedTopology[_ <: Entity]](
    deployedTopology: FDeployedTopology[DeployedTopology],
  )
  final case class PerEntityId[FDeployedTopology](
    deployedTopology: FDeployedTopology
  )
  type SameCtx[F[_ <: Entity]] = PerEntityCtx[F]
  type SameId[F] = PerEntityId[F]

  /**
    * Apply state-change results
    */
  type ApplyEntityStateChangeResults = SameCtx[ApplyStateChange.ApplyResults]
  object ApplyEntityStateChangeResults {
    def apply(
      deployedTopology: ApplyStateChange.ApplyResults[DeployedTopology] = IndexedSeq.empty
    ): ApplyEntityStateChangeResults = new ApplyEntityStateChangeResults(deployedTopology)
  }

  /**
    * Writable state stores
    */
  type WritableStateStores = PerEntityId[WritableStateStore[DeployedTopology, DeployedTopologyStateChange.StateChange]]
  object WritableStateStores {
    implicit class WritableStateAllDeploymentEntitiesExtensions(writableStateStores: WritableStateStores) {
      def applyEntityStateChange(changes: AllDeploymentEntitiesStateChanges): ApplyEntityStateChangeResults = {
        new ApplyEntityStateChangeResults(
          changes.deployedTopology.map(writableStateStores.deployedTopology.applyEntityStateChange)
        )
      }
    }
  }

  /**
    * Readable state stores
    */
  type ReadableStateStores = SameCtx[ReadableStateStore]

  /**
    * Readable versioned state stores
    */
  type ReadableVersionedStateStores = SameCtx[ReadableVersionedStateStore]
  object ReadableVersionedStateStores {
    implicit class AllDeploymentEntitiesReadableVersionedStateStoresExtensions(readableVersionedStateStore: ReadableVersionedStateStores) {
      def toReadable: ReadableStateStores = {
        new ReadableStateStores(
          readableVersionedStateStore.deployedTopology
        )
      }

      def withUncommittedChanges(
        changes: AllDeploymentEntitiesStateChanges
      )(implicit
        deployedTopologyApplyStateChange: ApplyStateChange[DeployedTopology, DeployedTopologyStateChange.StateChange],
      ): ReadableVersionedStateStores = {

        new ReadableVersionedStateStores(
          new ReadableVersionedStateStoreWithUncommittedChanges(readableVersionedStateStore.deployedTopology, changes.deployedTopology)
        )
      }
    }
  }

  /**
    * In-memory state stores
    */
  type InMemoryStateStores = PerEntityId[InMemoryStateStore[DeployedTopology, DeployedTopologyStateChange.StateChange]]
  object InMemoryStateStores {
    def apply(): InMemoryStateStores =
      new InMemoryStateStores(
        InMemoryStateStore.forDeployedTopology()
      )

    implicit class InMemoryStateStoreAllDeploymentEntitiesExtensions(inMemoryStateStores: InMemoryStateStores) {
      def toWritable: WritableStateStores = {
        new WritableStateStores(
          inMemoryStateStores.deployedTopology
        )
      }

      def toReadable: ReadableStateStores = {
        new ReadableStateStores(
          inMemoryStateStores.deployedTopology
        )
      }
    }
  }
}
