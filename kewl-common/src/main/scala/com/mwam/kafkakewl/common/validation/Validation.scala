/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import cats.data.Validated.Invalid
import cats.data.{NonEmptyList, ValidatedNel}
import cats.syntax.validated._
import com.mwam.kafkakewl.domain.{Command, CommandError, CommandResult, KafkaClusterCommand, KafkaClusterCommandResult, _}

object Validation {
  type Result = ValidatedNel[CommandError, Unit]
  object Result {
    def success: Result = ().validNel[CommandError]
    def error(failure: CommandError): Result = failure.invalidNel[Unit]
    def errors(failures: NonEmptyList[CommandError]): Result = Invalid(failures)
    def validationError(message: String): Result = error(CommandError.validationError(message))
    def permissionError(message: String): Result = error(CommandError.permissionError(message))

    def validationErrorIf(test: Boolean, message: String): Result = if (test) validationError(message) else success
  }

  implicit class ResultsExtensions(results: Iterable[Result]) {
    def combine(): Result = {
      if (results.forall(_.isValid)) {
        Result.success
      } else {
        Result.errors(NonEmptyList.fromList(results.collect { case Invalid(f) => f.toList}.reduce(_ ++ _)).get)
      }
    }
  }

  implicit class ResultExtensions(result: Result) {
    def ++(otherResult: Result): Result = List(result, otherResult).combine()

    def toCommandResultFailed(command: Command): Either[CommandResult.Failed, Unit] =
      result.toEither.left.map(f => command.failedResult(f.toList))

    def toKafkaClusterCommandResultFailed(command: KafkaClusterCommand): Either[KafkaClusterCommandResult.Failed, Unit] =
      result.toEither.left.map(f => command.failedResult(f.toList))

    def toCommandErrors: ValueOrCommandErrors[Unit] =
      result.toEither.left.map(_.toList)
  }
}
