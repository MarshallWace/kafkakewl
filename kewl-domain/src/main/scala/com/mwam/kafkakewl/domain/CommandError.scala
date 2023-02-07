/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import com.mwam.kafkakewl.utils._

sealed trait CommandErrorType {
  val errorType: String
  override def toString: String = errorType
}
object CommandErrorType {
  final case object NotImplemented extends CommandErrorType { val errorType = "NotImplemented" }
  final case object Permission extends CommandErrorType { val errorType = "PermissionError" }
  final case object Validation extends CommandErrorType { val errorType = "ValidationError" }
  final case object Exception extends CommandErrorType { val errorType = "Exception" }
  final case object Other extends CommandErrorType { val errorType = "OtherError" }
}

final case class CommandError(errorType: CommandErrorType = CommandErrorType.Other, message: String = "") {
  def mapMessage(f: String => String): CommandError = copy(message = f(message))

  override def toString: String = if (message.emptyIfNull.isEmpty) errorType.toString else s"$errorType: $message"
}
object CommandError {
  def apply(errorType: CommandErrorType = CommandErrorType.Other, message: String = ""): CommandError =
    new CommandError(errorType, message.emptyIfNull)

  def notImplementedError(message: String = "") = CommandError(CommandErrorType.NotImplemented, message)
  def permissionError(message: String = "") = CommandError(CommandErrorType.Permission, message)
  def validationError(message: String = "") = CommandError(CommandErrorType.Validation, message)
  def exception(message: String = "") = CommandError(CommandErrorType.Exception, message)
  def exception(throwable: Throwable) = CommandError(CommandErrorType.Exception, throwable.getMessage)
  def otherError(message: String = "") = CommandError(CommandErrorType.Other, message)
}
