/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.domain.deploy.{DeployedTopology, DeployedTopologyStateChange, Deployment, DeploymentStateChange}
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterStateChange}
import com.mwam.kafkakewl.domain.permission.{Permission, PermissionStateChange}
import com.mwam.kafkakewl.domain.topology.{Topology, TopologyStateChange}

import scala.collection.mutable

/**
  * A readable key-value pair store for Entities.
  *
  * @tparam E the type of the entity
  */
class InMemoryReadableStateStore[E <: Entity](initialStateStore: Iterable[EntityState[E]]) extends ReadableStateStore[E] {
  private[this] val store = initialStateStore.map(s => (s.id, s)).toMap

  def readableSnapshot: ReadableStateStore[E] = this // because it's immutable already
  def getLatestStates: IndexedSeq[EntityState[E]] = store.values.toIndexedSeq
  def getLatestState(id: String): Option[EntityState[E]] = store.get(id)
}

/**
  * A key-value pair store for Entities. It's not thread-safe, it assumes that the caller is single-threaded.
  *
  * @tparam E the type of the entity
  * @tparam C the type of the entity state change
  */
class InMemoryStateStore[E <: Entity, C <: EntityStateChange](initialStateStore: Iterable[EntityState[E]] = Seq.empty)(
  implicit applyStateChange: ApplyStateChange[E, C]
) extends WritableStateStore[E, C] with ReadableStateStore[E] {

  // storing the latest version of every id (even if that's deleted)
  private[this] val store = mutable.Map[String, EntityState[E]]() ++ initialStateStore.map(s => (s.id, s))

  def readableSnapshot: ReadableStateStore[E] = new InMemoryReadableStateStore(store.values)
  def getLatestStates: IndexedSeq[EntityState[E]] = store.values.toIndexedSeq
  def getLatestState(id: String): Option[EntityState[E]] = store.get(id)

  def applyEntityStateChange(change: C): ApplyStateChange.ApplyResult[E] = {
    val applyResult = applyStateChange.applyStateChange(this, change)
    applyResult match {
      case ApplyStateChange.ApplyResult(Some(s)) => store(s.id) = s
      case ApplyStateChange.ApplyResult(None) => ()
    }
    applyResult
  }
}

object InMemoryStateStore {
  def forPermissions(initialStateStore: Seq[EntityState[Permission]] = Seq.empty) =
    new InMemoryStateStore[Permission, PermissionStateChange.StateChange](initialStateStore)(ApplyStateChange.simple)
  def forTopology(initialStateStore: Seq[EntityState[Topology]] = Seq.empty) =
    new InMemoryStateStore[Topology, TopologyStateChange.StateChange](initialStateStore)(ApplyStateChange.simple)
  def forKafkaCluster(initialStateStore: Seq[EntityState[KafkaCluster]] = Seq.empty) =
    new InMemoryStateStore[KafkaCluster, KafkaClusterStateChange.StateChange](initialStateStore)(ApplyStateChange.simple)
  def forDeployment(initialStateStore: Seq[EntityState[Deployment]] = Seq.empty) =
    new InMemoryStateStore[Deployment, DeploymentStateChange.StateChange](initialStateStore)(ApplyStateChange.simple)
  def forDeployedTopology(initialStateStore: Seq[EntityState[DeployedTopology]] = Seq.empty) =
    new InMemoryStateStore[DeployedTopology, DeployedTopologyStateChange.StateChange](initialStateStore)(ApplyStateChange.simple)
}
