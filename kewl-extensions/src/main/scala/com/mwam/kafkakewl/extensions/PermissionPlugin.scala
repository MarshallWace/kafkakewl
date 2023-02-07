/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.extensions

import com.typesafe.config.Config

object UserPermission {
  /**
    * These types are mirrored from other types in com.mwam.kafkakewl.domain, which seems like code duplication, but it has advantages:
    *  - decoupling the public permissioning plugin's features from the internal kafkakewl permissioning features
    *    (features can be represented differently or missing, as long as there is a conversion from these to the internal permissions)
    *  - we can avoid depending on com.mwam.kafkakewl.domain
    */


  /**
    * The possible resource types for user-permissions (same as com.mwam.kafkakewl.domain.permission.PermissionResourceType)
    */
  sealed trait ResourceType
  object ResourceType {
    final case object System extends ResourceType
    final case object KafkaCluster extends ResourceType
    final case object Namespace extends ResourceType
    final case object Topology extends ResourceType
  }

  /**
    * The possible resource names for user-permissions (same as com.mwam.kafkakewl.domain.FlexibleName)
    */
  sealed trait ResourceName
  object ResourceName {
    final case class Any() extends ResourceName
    final case class Exact(exact: String) extends ResourceName
    final case class Regex(regex: String) extends ResourceName
    final case class Prefix(prefix: String) extends ResourceName
    final case class Namespace(namespace: String) extends ResourceName
  }

  /**
    * The possible resource operations for user-permissions (same as com.mwam.kafkakewl.domain.permission.PermissionResourceOperation)
    */
  sealed trait ResourceOperation
  object ResourceOperation {
    final case object Any extends ResourceOperation
    final case object Write extends ResourceOperation
    final case object Read extends ResourceOperation
    final case object Deploy extends ResourceOperation
    final case object ResetApplication extends ResourceOperation
  }
}

/**
  * A permission entity for a particular user-name.
  */
final case class UserPermission(
  userName: String,
  resourceType: UserPermission.ResourceType,
  resourceName: UserPermission.ResourceName,
  resourceOperations: Set[UserPermission.ResourceOperation] = Set(UserPermission.ResourceOperation.Any)
)

/**
  * A trait representing a permission-store that can return a set of permissions for a particular user.
  */
trait PermissionStore {
  def getPermissions(userName: String): Seq[UserPermission]
  def invalidateCachedPermissions(userName: Option[String] = None): Unit
}

/**
  * Extension interface to create a permission-store.
  */
trait PermissionPlugin extends Plugin {
  /**
    * Creates a permission-store that can return permissions for users.
    *
    * @param config the type-safe config to use to load any configuration for the plugin
    */
  def createPermissionStore(config: Config): PermissionStore
}
