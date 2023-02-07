/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

/**
  * Base trait for all entity state changes.
  */
trait EntityStateChange extends EntityStateMetadataOwner

/**
  * Base trait for the new-or-delete entity state changes.
  *
  * For most entity types we allow only these types of entity changes, no other special, entity-specific change-command.
  *
  * This is not sealed on purpose so that various entity types can extend it in other files.
  *
  * @tparam E the type of the entity
  */
trait SimpleEntityStateChange[E <: Entity] extends EntityStateChange

object SimpleEntityStateChange {
  /**
    * A completely new version for the given entity.
    *
    * @tparam E the type of the entity
    */
  trait NewVersion[E <: Entity] extends SimpleEntityStateChange[E] {
    val entity: E
  }

  /**
    * Deleting the given entity.
    *
    * @tparam E the type of the entity
    */
  trait Deleted[E <: Entity] extends SimpleEntityStateChange[E]

  final case class SimpleNewVersion[E <: Entity](metadata: EntityStateMetadata, entity: E) extends NewVersion[E]
  final case class SimpleDeleted[E <: Entity](metadata: EntityStateMetadata) extends Deleted[E]
}

/**
  * Represents an item in a transaction of state-changes. It's used for persistence.
  *
  * @param transactionId the id of the containing transaction
  * @param lastInTransaction true if this is the last one in the transaction
  * @param stateChanges the state entity changes
  */
final case class EntityStateChangeTransactionItem[SC](
  transactionId: String,
  lastInTransaction: Boolean,
  stateChanges: SC
)
