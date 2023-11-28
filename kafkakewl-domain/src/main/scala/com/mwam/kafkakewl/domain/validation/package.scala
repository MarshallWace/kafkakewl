/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import zio.prelude.Validation
import zio.*

import scala.annotation.targetName

package object validation {
  type ValidationError = String

  /** The validation does not produce anything, because the input is already a domain object that we're checking whether it's valid or not.
    *
    * TODO Maybe an abuse of the Validation[E, A] type? Ultimately all we need is a list of errors, when empty it means no errors.
    */
  type ValidationFailures = Validation[ValidationError, Unit]

  private val successValue: Unit = ()

  val valid: ValidationFailures = Validation.succeed(successValue)
  def invalid(validationError: ValidationError, validationErrors: ValidationError*): ValidationFailures =
    Validation.failNonEmptyChunk(NonEmptyChunk(validationError, validationErrors: _*))

  def validationErrorIf(error: => ValidationError)(predicate: => Boolean): ValidationFailures =
    Validation.fromPredicateWith(error)(successValue)(_ => !predicate)

  def combine(validationFailures1: ValidationFailures, validationFailures2: ValidationFailures): ValidationFailures =
    Validation.validateWith(validationFailures1, validationFailures2) { (_, _) => successValue }

  extension (validationFailures: ValidationFailures) {
    @targetName("add")
    def +(other: ValidationFailures): ValidationFailures = combine(validationFailures, other)
  }

  extension (validationFailures: Iterable[ValidationFailures]) {
    def combine: ValidationFailures = validationFailures.reduceOption(_ + _).getOrElse(valid)
  }
}
