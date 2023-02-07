/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import scala.reflect.runtime.universe._
import com.mwam.kafkakewl.common.ReadableStateStore
import com.mwam.kafkakewl.domain.{Entity, EntityId}
import com.mwam.kafkakewl.domain.Entity._
import com.mwam.kafkakewl.domain.permission.Permission

object EntityValidator {
  def validateExists[E <: Entity : TypeTag](
    stateStore: ReadableStateStore[E],
    id: EntityId
  ): Validation.Result = {
    if (!stateStore.hasLiveState(id)) {
      Validation.Result.validationError(s"${typeOf[E].entityName} $id does not exist")
    } else {
      Validation.Result.success
    }
  }

  def validateExists[E <: Entity : TypeTag](
    stateStore: ReadableStateStore[E],
    entity: E
  ): Validation.Result = {
    val matchingEntities = stateStore.getLatestLiveStates.filter(s => s.entity == entity)
    if (matchingEntities.isEmpty) {
      Validation.Result.validationError(s"${typeOf[Permission].entityName} $entity does not exist")
    } else {
      Validation.Result.success
    }
  }
}
