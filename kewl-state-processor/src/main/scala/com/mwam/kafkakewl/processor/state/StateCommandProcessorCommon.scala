/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.state

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.mwam.kafkakewl.common.ApplyStateChanges._
import com.mwam.kafkakewl.common.metrics.MetricsServiceOps
import com.mwam.kafkakewl.common.validation.{CommandProcessorValidator, PermissionValidator, Validation}
import com.mwam.kafkakewl.common.{AllStateEntities, CommandProcessorExtensions, ReadableStateStore}
import com.mwam.kafkakewl.domain.deploy.DeployedTopology
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.{Command, CommandResult}

import scala.concurrent.ExecutionContextExecutor

trait StateCommandProcessorCommon extends CommandProcessorExtensions
  with StateCommandProcessing
  with DeployedTopologyStateReadOnlyCommandProcessing {

  def processCommandWithStateStores(
    validateCommandFunc: (Command, AllStateEntities.ReadableVersionedStateStores) => Validation.Result,
    metricsService: Option[MetricsServiceOps]
  )(
    command: Command,
    permissionValidator: PermissionValidator,
    stateStores: AllStateEntities.ReadableVersionedStateStores,
    deployedTopologyStateStores: Map[KafkaClusterEntityId, ReadableStateStore[DeployedTopology]]
  )(implicit system: ActorSystem, ec: ExecutionContextExecutor, mat: Materializer): Either[CommandResult.Failed, StateCommandProcessingResult.Result] = {
    for {
      // permission and other validation
      _ <- permissionValidator.validateCommandPermissions(stateStores, command).toResultOrError(command)
      _ <- CommandProcessorValidator.validateCommand(command, stateStores, deployedTopologyStateStores).toResultOrError(command)
      _ <- validateCommandFunc(command, stateStores)
        .toEither
        .left.map(f => command.failedResult(f.toList))

      // processing, generating state changes
      processingResults <- command match {
        case c: Command.DeployedTopologyEntityCommand with Command.StateReadOnlyCommand =>
          process(c, metricsService, deployedTopologyStateStores).map(StateCommandProcessingResult.Result(_))
        case _ =>
          process(command, stateStores)
      }

      // generating additional kafka-cluster commands and more state changes
      changesAndCommands <- StateCommandProcessingKafkaCluster.getKafkaClusterCommands(
        command,
        stateStores,
        processingResults.changesAndCommands.entityChanges
      )
    } yield processingResults ++ changesAndCommands
  }

}
