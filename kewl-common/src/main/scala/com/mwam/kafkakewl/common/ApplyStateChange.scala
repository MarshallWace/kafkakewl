/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import com.mwam.kafkakewl.domain.{Entity, EntityState, EntityStateChange, SimpleEntityStateChange}

/**
  * A trait for type-classes that can apply state-changes into in-memory state stores.
  *
  * @tparam E the type of the entity
  * @tparam C the type of the entity state change
  */
trait ApplyStateChange[E <: Entity, C <: EntityStateChange] {
  def applyStateChange(readableStateStore: ReadableStateStore[E], change: C): ApplyStateChange.ApplyResult[E]
}

/**
  * Types for the ApplyStateChange type class trait.
  */
object ApplyStateChange{

  /**
    * The result of applying a state-change is really just an optional EntityState[E] instance which covers deletions and new versions too.
    *
    * @param state the new entity state as the result of the state change.
    * @tparam E the type of the entity
    */
  final case class ApplyResult[E <: Entity](state: Option[EntityState[E]])

  /**
    * A type alias to be able to have a type constructor for an IndexedSeq of ApplyResults.
    *
    * @tparam E the type of the entity
    */
  type ApplyResults[E <: Entity] = IndexedSeq[ApplyResult[E]]

  /**
    * Creates a state-change applier type class instance that can deal with the SimpleEntityStateChange[E] changes (i.e. new-version or delete changes)
    *
    * @tparam E the type of the entity
    * @return an ApplyStateChange instance
    */
  def simple[E <: Entity, C <: SimpleEntityStateChange[E]]: ApplyStateChange[E, C] = {
    //noinspection ConvertExpressionToSAM
    new ApplyStateChange[E, C]() {
      override def applyStateChange(readableStateStore: ReadableStateStore[E], change: C): ApplyResult[E] = {
        (simplePF(readableStateStore) orElse nonePF[E, C])(change)
      }
    }
  }

  type PF[E <: Entity, C <: SimpleEntityStateChange[E]] = PartialFunction[C, ApplyResult[E]]

  /**
    * Creates a partial function that matches everything and returns ApplyResult.None.
    *
    * @tparam E the type of the entity
    * @tparam C the type of the entity state change
    * @return a partial function that matches everything and returns ApplyResult.None.
    */
  def nonePF[E <: Entity, C <: SimpleEntityStateChange[E]]: PF[E, C] = { case _ => ApplyResult(None) }

  /**
    * Creates a partial function that matches the simple new version and delete state changes only.
    *
    * @param readableStateStore the readable store (unused)
    * @tparam E the type of the entity
    * @tparam C the type of the state changes
    * @return a partial function processing SimpleEntityStateChange.NewVersion and SimpleEntityStateChange.Deleted only
    */
  def simplePF[E <: Entity, C <: SimpleEntityStateChange[E]](readableStateStore: ReadableStateStore[E]): PF[E, C] = {
    case v: SimpleEntityStateChange.NewVersion[E] => ApplyResult[E](Some(EntityState.Live[E](v.metadata, v.entity)))
    case d: SimpleEntityStateChange.Deleted[E] => ApplyResult[E](Some(EntityState.Deleted[E](d.metadata)))
  }
}
