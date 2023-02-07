/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import com.mwam.kafkakewl.common.{AllStateEntities, ReadableStateStore}
import com.mwam.kafkakewl.common.validation.permissions._
import com.mwam.kafkakewl.common.validation.Validation.Result
import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.domain.permission.Permission
import com.typesafe.scalalogging.LazyLogging

object PermissionValidator {
  type CreatePermissionStoreBuiltin = ReadableStateStore[Permission] => PermissionStoreBuiltin
}

class PermissionValidator(
  superUsers: Seq[String],
  createPermissionStore: PermissionValidator.CreatePermissionStoreBuiltin = PermissionStoreBuiltin.apply
) extends LazyLogging {
  private[this] val superUsersSet = superUsers.map(_.toLowerCase).toSet

  private def validatePermissions(
    permissions: Seq[Permission],
    userName: String,
    requirements: Iterable[PermissionRequirement],
    permissionSubjectDescription: String
  ): Validation.Result = {
    val lowerCaseUserName = userName.toLowerCase
    val isSuperUser = superUsersSet.contains(lowerCaseUserName)

    // super-user checking
    val superUserAllowed = isSuperUser
    val whySuperUserAllowed = if (isSuperUser) Seq(s"$lowerCaseUserName being superuser") else Seq.empty

    // checking the requirements against the current permissions and user
    val requirementsMatchResults = requirements.map(_.doesMatch(lowerCaseUserName, permissions))
    val requirementsMatchResult = PermissionRequirement.MatchResults.foldLeftAnd(requirementsMatchResults)
    val allowed = superUserAllowed || requirementsMatchResult.doesMatch

    if (allowed) {
      val whyAllowed = whySuperUserAllowed ++ requirementsMatchResult.reasons
      logger.debug(s"allowed because of ${whyAllowed.mkString(", ")}: $permissionSubjectDescription")
    } else {
      logger.debug(s"NOT allowed: $permissionSubjectDescription")
    }

    if (allowed)
      Validation.Result.success
    else
      Validation.Result.permissionError(s"no permission for $lowerCaseUserName for $permissionSubjectDescription.")
  }

  def validateCommandPermissions(
    stateStores: AllStateEntities.ReadableVersionedStateStores,
    command: Command
  ): Result = {
    val requirements = PermissionRequirements(stateStores).forCommand(command)
    validatePermissions(
      createPermissionStore(stateStores.permission).getPermissions(command.userName),
      command.userName,
      requirements,
      s"${command.getClass.getSimpleName}[${requirements.map(_.toString).mkString("; ")}]"
    )
  }

  def validateCommandPermissions(
    stateStores: AllStateEntities.ReadableVersionedStateStores,
    command: KafkaClusterCommand
  ): Result = {
    val requirements = PermissionRequirements(stateStores).forCommand(command)
    validatePermissions(
      createPermissionStore(stateStores.permission).getPermissions(command.userName),
      command.userName,
      requirements,
      s"${command.getClass.getSimpleName}[${requirements.map(_.toString).mkString("; ")}]"
    )
  }

  def validateEntityPermissions(
    userName: String,
    stateStores: AllStateEntities.ReadableVersionedStateStores,
    id: String,
    entity: Entity
  ): Result = {
    val requirements = PermissionRequirements(stateStores).forEntity(id, entity)
    validatePermissions(
      createPermissionStore(stateStores.permission).getPermissions(userName),
      userName,
      requirements,
      s"${entity.getClass.getSimpleName.toLowerCase}#$id"
    )
  }
}
