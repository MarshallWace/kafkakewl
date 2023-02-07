/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import com.mwam.kafkakewl.domain._

/**
  * A state store trait that can apply an entity's state change. Its implementations can be e.g. in-memory or persistent in different ways.
  *
  * @tparam C the type of the entity state change
  */
trait WritableStateStore[E <: Entity, C <: EntityStateChange] {
  /**
    * Applies the specified entity state change.
    *
    * @param change the new entity state change to store apply
    */
  def applyEntityStateChange(change: C): ApplyStateChange.ApplyResult[E]
}

/**
  * A state store trait that can return the current state of entities by their id (random access).
  *
  * @tparam E the type of the entity
  */
trait ReadableStateStore[E <: Entity] {
  /**
    * Clones the current state store (only the mutable part, does not deep-clone the immutable objects - that would be pointless)
    *
    * @return the clone of the current state store.
    */
  def readableSnapshot: ReadableStateStore[E]

  /**
    * Gets all the current entity states (even deleted ones).
    *
    * @return all the current entity states.
    */
  def getLatestStates: IndexedSeq[EntityState[E]]

  /**
    * Gets all the current live (non-deleted) entity states.
    *
    * @return all the current entity states.
    */
  def getLatestLiveStates: IndexedSeq[EntityState.Live[E]] = getLatestStates.collect { case s: EntityState.Live[E] => s }

  /**
    * Gets the entity state (deleted one too) for the given id or returns None if there isn't any entity with that id.
    *
    * @param id the id of the entity.
    * @return the entity's state of this id
    */
  def getLatestState(id: String): Option[EntityState[E]]

  /**
    * Gets the next version for the given entity id.
    *
    * @param id the id of the entity.
    * @return the next version for the given entity id
    */
  def getNextStateVersion(id: String): Int = getLatestState(id).map(_.version + 1).getOrElse(1)

  /**
    * Gets the live entity state for the given id or returns None if there isn't any live entity with that id.
    *
    * @param id the id of the entity.
    * @return the entity's state of this id
    */
  def getLatestLiveState(id: String): Option[EntityState.Live[E]] = getLatestState(id).flatMap(_.liveOrNone)

  /**
    * Returns true if there is a live entity state for the specified id.
    *
    * @param id the id.
    * @return True if there is a live entity state for the specified id.
    */
  def hasLiveState(id: String): Boolean = getLatestLiveState(id).isDefined

  // TODO this is where we lose the type-safety of the entity-specific ids...
  def getLatestState(id: EntityId): Option[EntityState[E]] = getLatestState(id.id)
  def getNextStateVersion(id: EntityId): Int = getNextStateVersion(id.id)
  def getLatestLiveState(id: EntityId): Option[EntityState.Live[E]] = getLatestLiveState(id.id)
  def hasLiveState(id: EntityId): Boolean = hasLiveState(id.id)
}

/**
  * A state store trait that can return all the versioned states of entities by their id (random access).
  *
  * @tparam E the type of the entity
  */
trait ReadableVersionedStateStore[E <: Entity] extends ReadableStateStore[E] {
  /**
    * Clones the current state store (only the mutable part, does not deep-clone the immutable objects - that would be pointless)
    *
    * @return the clone of the current state store.
    */
  def readableSnapshot: ReadableVersionedStateStore[E]

  /**
    * Gets the entity states for the given id or returns empty list if there isn't any entity with that id.
    *
    * @param id the id of the entity.
    * @return the entity's states of this id
    */
  def getStateVersions(id: String): IndexedSeq[EntityState[E]]

  /**
    * Gets the entity state for the given id and version or returns None if it doesn't exist.
    *
    * @param id the id of the entity.
    * @param version the version of the entity state
    * @return the entity's state of this id and version
    */
  def getStateVersion(id: String, version: Int): Option[EntityState[E]]

  /**
    * Gets the live entity state for the given id and version or returns None if it doesn't exist (or not Live).
    *
    * @param id the id of the entity.
    * @param version the version of the entity state
    * @return the entity's live state of this id and version
    */
  def getLiveStateVersion(id: String, version: Int): Option[EntityState.Live[E]] = for {
    state <- getStateVersion(id, version)
    liveState <- state match {
      case s: EntityState.Live[E] => Some(s)
      case _ => None
    }
  } yield liveState

  // TODO this is where we lose the type-safety of the entity-specific ids...
  def getStateVersions(id: EntityId): IndexedSeq[EntityState[E]] = getStateVersions(id.id)
  def getStateVersion(id: EntityId, version: Int): Option[EntityState[E]] = getStateVersion(id.id, version)
  def getLiveStateVersion(id: EntityId, version: Int): Option[EntityState.Live[E]] = getLiveStateVersion(id.id, version)
}
