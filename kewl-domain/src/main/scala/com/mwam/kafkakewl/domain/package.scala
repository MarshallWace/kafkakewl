/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl

import cats.data.{NonEmptyList, ValidatedNel}
import cats.syntax.either._

import scala.util.{Failure, Success, Try}

package object domain {
  type ValueOrCommandError[T] = Either[CommandError, T]
  type ValueOrCommandErrors[T] = Either[Seq[CommandError], T]
  type ValidatedValueOrCommandErrors[T] = ValidatedNel[CommandError, T]

  implicit class ValueOrCommandErrorExtensions[T](v: ValueOrCommandError[T]) {
    def toCommandErrors: ValueOrCommandErrors[T] = v.left.map(ce => Seq(ce))
    def toValidatedNel: ValidatedValueOrCommandErrors[T] = v.toValidated.leftMap(NonEmptyList.one)
  }

  implicit class ValueOrCommandErrorsExtensions[T](v: ValueOrCommandErrors[T]) {
    def toValidatedValueOrCommandErrors: ValidatedValueOrCommandErrors[T] = v.toValidated.leftMap(ce => NonEmptyList.fromListUnsafe(ce.toList))
  }

  implicit class ValidatedValueOrCommandErrorsExtensions[T](v: ValidatedValueOrCommandErrors[T]) {
    def toValueOrCommandErrors: ValueOrCommandErrors[T] = v.leftMap(_.toList).toEither
  }

  implicit class DomainTryExtensions[T](t: Try[T]) {
    def toRightOrCommandError: ValueOrCommandError[T] = {
      t match {
        case Success(r) => Right(r)
        case Failure(e) => Left(CommandError.exception(e))
      }
    }

    def toRightOrCommandErrors: ValueOrCommandErrors[T] = {
      t match {
        case Success(r) => Right(r)
        case Failure(e) => Left(Seq(CommandError.exception(e)))
      }
    }
  }
}
