/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import cats.syntax.either._
import cats.data.Validated
import com.mwam.kafkakewl.common.validation.PermissionValidator
import com.mwam.kafkakewl.domain.{Command, CommandResponse, CommandResult}

trait PermissionCommandResultCommon {
  def applyPermissionsToCommandResult(
    permissionValidator: PermissionValidator,
    stateStores: AllStateEntities.ReadableVersionedStateStores
  )(
    command: Command,
    commandResult: CommandResult.Succeeded
  ): Either[CommandResult.Failed, CommandResult.Succeeded] = {
    commandResult.response match {
      case r: CommandResponse.StateBase =>
        permissionValidator.validateEntityPermissions(command.userName, stateStores, r.state.id, r.state.entity) match {
          // no permission for the entity we're returning -> change the result to be an error
          case Validated.Invalid(f) => command.failedResult(f.toList).asLeft
          case _ => commandResult.asRight
        }

      case r: CommandResponse.StateListBase =>
        // filtering the entity-states result with the permission-filter
        val filteredResponse = r.filter(s => permissionValidator.validateEntityPermissions(command.userName, stateStores, s.id, s.entity).isValid)
        commandResult.copy(response = filteredResponse).asRight

      case r: CommandResponse.ResolvedTopologies =>
        // filtering the entity-states result with the permission-filter
        val filteredResponse = r.filter((id, s) => permissionValidator.validateEntityPermissions(command.userName, stateStores, id.id, s).isValid)
        commandResult.copy(response = filteredResponse).asRight

      case r: CommandResponse.ResolvedDeployedTopologies =>
        // filtering the entity-states result with the permission-filter
        val filteredResponse = r.filter((id, s) => permissionValidator.validateEntityPermissions(command.userName, stateStores, id.id, s).isValid)
        commandResult.copy(response = filteredResponse).asRight

      case r: CommandResponse.DeployedTopologiesMetricsResponse =>
        val filteredDeployedTopologiesMetrics = r.deployedTopologiesMetrics.filter(dtm => permissionValidator.validateEntityPermissions(command.userName, stateStores, dtm.id.id, dtm).isValid)
        commandResult.copy(response = r.copy(deployedTopologiesMetrics = filteredDeployedTopologiesMetrics)).asRight

      case r: CommandResponse.DeployedTopologiesMetricsCompactResponse =>
        val filteredDeployedTopologiesMetrics = r.deployedTopologiesMetrics.filter(dtm => permissionValidator.validateEntityPermissions(command.userName, stateStores, dtm.id.id, dtm).isValid)
        commandResult.copy(response = r.copy(deployedTopologiesMetrics = filteredDeployedTopologiesMetrics)).asRight

      case r: CommandResponse.DeployedTopologyMetricsResponse =>
        permissionValidator.validateEntityPermissions(command.userName, stateStores, r.deployedTopologyMetrics.id.id, r.deployedTopologyMetrics) match {
          // no permission for the entity we're returning -> change the result to be an error
          case Validated.Invalid(f) => command.failedResult(f.toList).asLeft
          case _ => commandResult.asRight
        }

      case r: CommandResponse.DeployedTopologyMetricsCompactResponse =>
        permissionValidator.validateEntityPermissions(command.userName, stateStores, r.deployedTopologyMetrics.id.id, r.deployedTopologyMetrics) match {
          // no permission for the entity we're returning -> change the result to be an error
          case Validated.Invalid(f) => command.failedResult(f.toList).asLeft
          case _ => commandResult.asRight
        }

      case _ => commandResult.asRight
    }
  }
}
