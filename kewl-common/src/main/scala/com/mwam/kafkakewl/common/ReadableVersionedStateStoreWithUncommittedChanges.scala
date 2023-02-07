/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import com.mwam.kafkakewl.domain._

import scala.collection.mutable

/**
  * An implementation of ReadableVersionedStateStore that has an underlying ReadableVersionedStateStore
  * and a set of changes that are not yet committed into the state-store.
  *
  * @param stateStore the state store
  * @param changes the state changes that are not yet committed into the state store
  *
  * @tparam E the type of the entity
  * @tparam C the type of the entity state change
  */
class ReadableVersionedStateStoreWithUncommittedChanges[E <: Entity, C <: EntityStateChange](
  private[this] val stateStore: ReadableVersionedStateStore[E],
  private[this] val changes: IndexedSeq[C]
)(implicit applyStateChange: ApplyStateChange[E, C]) extends ReadableVersionedStateStore[E] {

  private def latestStates(): mutable.Map[String, EntityState[E]] = {
    val ls = mutable.Map() ++ stateStore.getLatestStates.map(s => (s.id, s)).toMap
    changes
      .flatMap(applyStateChange.applyStateChange(stateStore, _).state)
      .foreach(s => ls(s.id) = s)
    ls
  }

  private def stateVersions(id: String): IndexedSeq[EntityState[E]] = {
    val sv = stateStore.getStateVersions(id)
    sv ++ changes
      .filter(_.id == id)
      .flatMap(applyStateChange.applyStateChange(stateStore, _).state)
  }

  def readableSnapshot: ReadableVersionedStateStore[E] = this // because it's immutable already

  def getLatestStates: IndexedSeq[EntityState[E]] = latestStates().values.toIndexedSeq
  def getLatestState(id: String): Option[EntityState[E]] = latestStates().get(id)

  def getStateVersions(id: String): IndexedSeq[EntityState[E]] = stateVersions(id)
  def getStateVersion(id: String, version: Int): Option[EntityState[E]] = stateVersions(id).find(_.version == version)
}
