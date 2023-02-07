/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import com.mwam.kafkakewl.common.ReadableStateStore
import com.mwam.kafkakewl.domain.FlexibleName
import com.mwam.kafkakewl.domain.permission.{Permission, PermissionResourceOperation, PermissionResourceType}
import com.mwam.kafkakewl.extensions
import com.typesafe.scalalogging.LazyLogging

/**
  * A trait representing a permission-store that can return a set of permissions for a particular user.
  *
  * It's different from com.mwam.kafkakewl.extensions.PermissionStore in that it returns com.mwam.kafkakewl.domain.permission.Permission.
  */
trait PermissionStoreBuiltin {
  def name: String
  def getPermissions(userName: String): Seq[Permission]
  def invalidateCachedPermissions(userName: Option[String] = None): Unit
}

object PermissionStoreBuiltin extends LazyLogging {
  /**
    * Creates a PermissionStoreBuiltin instance that gets the permissions from the readable state store for permissions.
    *
    * @param permissionsStateStore the readable state store for permissions
    */
  def apply(permissionsStateStore: ReadableStateStore[Permission]) =
    new PermissionStoreBuiltin {
      override def name = "built-in"
      override def getPermissions(userName: String): Seq[Permission] = {
        // here I just ignore the userName because ultimately the permission-checking logic uses only those permissions
        permissionsStateStore.getLatestLiveStates.map(_.entity)
      }
      override def invalidateCachedPermissions(userName: Option[String] = None): Unit = {}
    }

  /**
    * Creates a PermissionStoreBuiltin instance that gets the permissions from the external permission store.
    *
    * @param permissionStoreExt the external permission store
    */
  def apply(permissionStoreExt: extensions.PermissionStore) =
    new PermissionStoreBuiltin {
      override def name = "extension"
      override def getPermissions(userName: String): Seq[Permission] = {
        val resourceType: extensions.UserPermission.ResourceType => PermissionResourceType = {
          case extensions.UserPermission.ResourceType.System => PermissionResourceType.System
          case extensions.UserPermission.ResourceType.KafkaCluster => PermissionResourceType.KafkaCluster
          case extensions.UserPermission.ResourceType.Namespace => PermissionResourceType.Namespace
          case extensions.UserPermission.ResourceType.Topology => PermissionResourceType.Topology
        }

        val resourceName: extensions.UserPermission.ResourceName => FlexibleName = {
          case extensions.UserPermission.ResourceName.Any() => FlexibleName.Any()
          case extensions.UserPermission.ResourceName.Exact(v) => FlexibleName.Exact(v)
          case extensions.UserPermission.ResourceName.Regex(v) => FlexibleName.Regex(v)
          case extensions.UserPermission.ResourceName.Prefix(v) => FlexibleName.Prefix(v)
          case extensions.UserPermission.ResourceName.Namespace(v) => FlexibleName.Namespace(v)
        }

        val resourceOperation: extensions.UserPermission.ResourceOperation => PermissionResourceOperation = {
          case extensions.UserPermission.ResourceOperation.Any => PermissionResourceOperation.Any
          case extensions.UserPermission.ResourceOperation.Write => PermissionResourceOperation.Write
          case extensions.UserPermission.ResourceOperation.Read => PermissionResourceOperation.Read
          case extensions.UserPermission.ResourceOperation.Deploy => PermissionResourceOperation.Deploy
          case extensions.UserPermission.ResourceOperation.ResetApplication => PermissionResourceOperation.ResetApplication
        }

        def permission(userPermissionExt: extensions.UserPermission) = Permission(
          userPermissionExt.userName,
          resourceType(userPermissionExt.resourceType),
          resourceName(userPermissionExt.resourceName),
          userPermissionExt.resourceOperations.map(resourceOperation),
        )

        permissionStoreExt.getPermissions(userName).map(permission)
      }
      override def invalidateCachedPermissions(userName: Option[String] = None): Unit = permissionStoreExt.invalidateCachedPermissions(userName)
    }
}