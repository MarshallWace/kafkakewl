/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package permission

final case class PermissionEntityId(id: String) extends AnyVal with EntityId

/**
  * An entity type representing a permission.
  *
  * @param principal the principal who's permissioned
  * @param resourceType the resource type
  * @param resourceOperations the resource operations
  * @param resourceName the resource flexible name
  */
final case class Permission(
  principal: String,
  resourceType: PermissionResourceType,
  resourceName: FlexibleName,
  resourceOperations: Set[PermissionResourceOperation] = Set(PermissionResourceOperation.Any)
) extends Entity {

  def equivalent(that: Permission): Boolean =
    principal == that.principal &&
      resourceType == that.resourceType &&
      resourceName == that.resourceName &&
      resourceOperations == that.resourceOperations
}

/**
  * An enumeration for the resource types that can be permissioned.
  */
sealed trait PermissionResourceType
object PermissionResourceType {
  final case object System extends PermissionResourceType
  final case object KafkaCluster extends PermissionResourceType
  final case object Namespace extends PermissionResourceType
  final case object Topology extends PermissionResourceType
}

/**
  * An enumeration for the resource types operations for permissions. At the moment it's only Any, but can be extended if needed.
  */
sealed trait PermissionResourceOperation
object PermissionResourceOperation {
  final case object Any extends PermissionResourceOperation
  final case object Write extends PermissionResourceOperation
  final case object Read extends PermissionResourceOperation
  final case object Deploy extends PermissionResourceOperation
  final case object ResetApplication extends PermissionResourceOperation
}

object PermissionStateChange {
  sealed trait StateChange extends SimpleEntityStateChange[Permission]
  final case class NewVersion(metadata: EntityStateMetadata, entity: Permission) extends StateChange with SimpleEntityStateChange.NewVersion[Permission]
  final case class Deleted(metadata: EntityStateMetadata) extends StateChange with SimpleEntityStateChange.Deleted[Permission]
}
