/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import cats.syntax.either._
import com.mwam.kafkakewl.common.validation.Validation
import com.mwam.kafkakewl.domain.{Command, CommandResult}

import scala.concurrent.{ExecutionContextExecutor, Future}

trait CommandProcessorExtensions {
  type ValueOrCommandError[T] = Either[CommandResult.Failed, T]
  type ValidationResultOrError = ValueOrCommandError[Unit]
  type CommandResultOrError = ValueOrCommandError[CommandResult.Succeeded]

  implicit class CommandResultSucceededExtensions(commandResult: CommandResult.Succeeded) {
    def toResultOrError: CommandResultOrError = commandResult.asRight[CommandResult.Failed]
  }

  implicit class CommandResultFailedExtensions(commandResult: CommandResult.Failed) {
    def toResultOrError: CommandResultOrError = commandResult.asLeft[CommandResult.Succeeded]
  }

  implicit class ValidationResultExtensions(validationResult: Validation.Result) {
    def toResultOrError(command: Command): ValidationResultOrError = validationResult.toEither.leftMap(f => command.failedResult(f.toList))
  }

  implicit class CommandResultOrErrorFutureExtensions(commandResultOrError: Future[CommandResultOrError])(implicit val ec: ExecutionContextExecutor) {
    def toCommandResult: Future[CommandResult] = commandResultOrError.map(_.merge)
  }
}
