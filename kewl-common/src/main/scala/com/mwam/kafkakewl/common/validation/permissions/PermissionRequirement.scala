/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation.permissions

import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.permission.{Permission, PermissionResourceOperation, PermissionResourceType}
import com.mwam.kafkakewl.domain.topology.{Namespace, TopologyEntityId}

sealed trait PermissionRequirement {
  def doesMatch(lowerCaseUserName: String, permissions: Seq[Permission]): PermissionRequirement.MatchResult
}
object PermissionRequirement {
  final case class MatchResult(doesMatch: Boolean, reasons: Seq[String]) {
    def &(other: MatchResult): MatchResult = MatchResult(doesMatch && other.doesMatch, reasons ++ other.reasons)
    def |(other: MatchResult): MatchResult = MatchResult(doesMatch || other.doesMatch, reasons ++ other.reasons)
  }

  object MatchResults {
    def foldLeftAnd(matchResults: Iterable[MatchResult]): MatchResult = {
      // can't just do foldLeft with this default, because the default reason is not needed if there is more than one
      if (matchResults.isEmpty)
        PermissionRequirement.MatchResult(doesMatch = true, Seq("no permission required"))
      else
        matchResults.reduceLeft(_ & _)
    }

    def foldLeftOr(matchResults: Iterable[MatchResult]): MatchResult = {
      // can't just do foldLeft with this default, because the default reason is not needed if there is more than one (as well the as the doesMatch = true wouldn't make sense)
      if (matchResults.isEmpty)
        PermissionRequirement.MatchResult(doesMatch = true, Seq("no permission required"))
      else
        matchResults.reduceLeft(_ | _)
    }
  }

  final case class EitherOneOf(requirements: PermissionRequirement*) extends PermissionRequirement {
    def doesMatch(lowerCaseUserName: String, permissions: Seq[Permission]): PermissionRequirement.MatchResult = {
      MatchResults.foldLeftOr(requirements.map(_.doesMatch(lowerCaseUserName, permissions)))
    }

    override def toString: String = requirements.map(_.toString).mkString("(", " OR ", ")")
  }

  /**
    * A resource and an operation on it. It's used for permissions.
    *
    * @param resourceType the type of the resource
    * @param resourceName the name of the resource
    *                     if None, then any permission for the resource type and operation is enough
    * @param resourceOperation the operation on the resource
    */
  final case class PermissionResourceAndOperation(
    resourceType: PermissionResourceType,
    resourceName: Option[String],
    resourceOperation: PermissionResourceOperation
  ) extends PermissionRequirement {
    private def doesMatch(lowerCaseUserName: String, permission: Permission): Boolean = {
      // everything is matched case-insensitive
      val matchingPrincipal = permission.principal.toLowerCase == lowerCaseUserName
      val matchingResourceType = permission.resourceType == resourceType
      val matchingResourceOperation =
        permission.resourceOperations.contains(PermissionResourceOperation.Any) ||
          permission.resourceOperations.contains(resourceOperation)
      // if the resource name is not None, it must match the flexible name in the permission, otherwise it's a match
      val matchingResourceName = resourceName.forall(permission.resourceName.doesMatch)
      // all must match
      matchingPrincipal && matchingResourceType && matchingResourceOperation && matchingResourceName
    }

    def doesMatch(lowerCaseUserName: String, permissions: Seq[Permission]): MatchResult = {
      val matchingPermissions = permissions.filter(doesMatch(lowerCaseUserName, _))
      val reasons = matchingPermissions.map(_.toString)
      MatchResult(matchingPermissions.nonEmpty, reasons)
    }

    override def toString: String = resourceName
      .map(rn => s"$resourceOperation:$resourceType#$rn")
      .getOrElse(s"$resourceOperation:$resourceType")
  }

  /**
    * A permission requirement that expects the principal to match the specified one OR the user to have Read permission for System.
    *
    * @param principal only this principal
    */
  final case class PrincipalOrSystemRead(
    principal: String
  ) extends PermissionRequirement {
    private val lowerCasePrincipal = principal.toLowerCase

    def doesMatch(lowerCaseUserName: String, permissions: Seq[Permission]): MatchResult = {
      val matchingPrincipal = lowerCaseUserName == lowerCasePrincipal
      val systemReadPermission = permissions.find(p =>
        p.principal.toLowerCase == lowerCaseUserName &&
          p.resourceType == PermissionResourceType.System &&
          (p.resourceOperations.contains(PermissionResourceOperation.Read) || p.resourceOperations.contains(PermissionResourceOperation.Any)))
      MatchResult(
        matchingPrincipal || systemReadPermission.nonEmpty,
        Seq(
          if (matchingPrincipal) Some(s"$lowerCaseUserName is expected principal") else None,
          systemReadPermission.map(_.toString)
        ).flatten
      )
    }

    override def toString: String = s"owned by $principal"
  }

  val systemRead = PermissionResourceAndOperation(PermissionResourceType.System, None, PermissionResourceOperation.Read)
  val systemAny = PermissionResourceAndOperation(PermissionResourceType.System, None, PermissionResourceOperation.Any)

  def topology(id: TopologyEntityId, topologyNamespace: Namespace, resourceOperation: PermissionResourceOperation) =
    EitherOneOf(
      // topology operations are permissioned by the namespace of the topology
      PermissionResourceAndOperation(PermissionResourceType.Namespace, Some(topologyNamespace.ns), resourceOperation),
      // or the topology entity id itself
      PermissionResourceAndOperation(PermissionResourceType.Topology, Some(id.id), resourceOperation)
    )

  def kafkaCluster(id: KafkaClusterEntityId, kafkaCluster: KafkaCluster, resourceOperation: PermissionResourceOperation) =
    // kafka-cluster operations are permissioned by the id of the kafka-cluster
    PermissionResourceAndOperation(PermissionResourceType.KafkaCluster, Some(id.id), resourceOperation)
}
