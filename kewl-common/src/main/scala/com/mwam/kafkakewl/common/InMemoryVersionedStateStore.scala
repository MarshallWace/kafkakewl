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

import scala.collection.{SortedMap, mutable}

/**
  * A versioned readable key-value pair store for Entities.
  *
  * @tparam E the type of the entity
  */
class InMemoryReadableVersionedStateStore[E <: Entity](initialStateStore: Iterable[(String, Iterable[(Int, EntityState[E])])]) extends ReadableStateStore[E] with ReadableVersionedStateStore[E] {
  private[this] val store = initialStateStore.map { case (id, versions) => (id, SortedMap.empty[Int, Nothing] ++ versions)}.toMap

  def readableSnapshot: ReadableVersionedStateStore[E] = this // because it's immutable already
  def getLatestStates: IndexedSeq[EntityState[E]] = store.values.map(_.last._2).toIndexedSeq
  def getLatestState(id: String): Option[EntityState[E]] = store.get(id).map(_.last._2)

  def getStateVersions(id: String): IndexedSeq[EntityState[E]] = store.get(id).map(_.values.toIndexedSeq).getOrElse(IndexedSeq.empty)
  def getStateVersion(id: String, version: Int): Option[EntityState[E]] =
    for {
      entityVersions <- store.get(id)
      entityVersion <- entityVersions.get(version)
    } yield entityVersion
}

/**
  * A versioned key-value pair store for Entities. It's not thread-safe, it assumes that the caller is single-threaded.
  *
  * @tparam E the type of the entity
  * @tparam C the type of the entity state change
  */
class InMemoryVersionedStateStore[E <: Entity, C <: EntityStateChange](
  implicit applyStateChange: ApplyStateChange[E, C]
) extends WritableStateStore[E, C] with ReadableStateStore[E] with ReadableVersionedStateStore[E] {

  // for every id, it contains all versions' states sorted by version number (we have a version even for deleted-states)
  private[this] val store = mutable.Map[String, mutable.SortedMap[Int, EntityState[E]]]()

  def readableSnapshot: ReadableVersionedStateStore[E] = new InMemoryReadableVersionedStateStore(store)
  def getLatestStates: IndexedSeq[EntityState[E]] = store.values.map(_.last._2).toIndexedSeq
  def getLatestState(id: String): Option[EntityState[E]] = store.get(id).map(_.last._2)

  def getStateVersions(id: String): IndexedSeq[EntityState[E]] = store.get(id).map(_.values.toIndexedSeq).getOrElse(IndexedSeq.empty)
  def getStateVersion(id: String, version: Int): Option[EntityState[E]] =
    for {
      entityVersions <- store.get(id)
      entityVersion <- entityVersions.get(version)
    } yield entityVersion

  def applyEntityStateChange(change: C): ApplyStateChange.ApplyResult[E] = {
    val applyResult = applyStateChange.applyStateChange(this, change)
    applyResult match {
      // we treat live and deleted states the same way
      case ApplyStateChange.ApplyResult(Some(s)) =>
        store.get(s.id) match {
          case Some(versions) => versions.put(s.version, s)
          case None => store(s.id) = mutable.SortedMap[Int, EntityState[E]](s.version -> s)
        }
      case ApplyStateChange.ApplyResult(None) => ()
    }
    applyResult
  }
}

object InMemoryVersionedStateStore {
  def forPermission() = new InMemoryVersionedStateStore[Permission, PermissionStateChange.StateChange]()(ApplyStateChange.simple)
  def forTopology() = new InMemoryVersionedStateStore[Topology, TopologyStateChange.StateChange]()(ApplyStateChange.simple)
  def forKafkaCluster() = new InMemoryVersionedStateStore[KafkaCluster, KafkaClusterStateChange.StateChange]()(ApplyStateChange.simple)
  def forDeployment() = new InMemoryVersionedStateStore[Deployment, DeploymentStateChange.StateChange]()(ApplyStateChange.simple)
  def forDeployedTopology() = new InMemoryVersionedStateStore[DeployedTopology, DeployedTopologyStateChange.StateChange]()(ApplyStateChange.simple)
}
