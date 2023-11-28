/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.domain

/** Base trait for failures while querying something.
  */
sealed trait QueryFailure

object Failures {
  final case class NotFound(notFound: Seq[String]) extends QueryFailure
  final case class Authorization(authorizationFailed: Seq[String]) extends QueryFailure

  def notFound(notFound: String*): NotFound = NotFound(notFound)
  def authorization(throwable: Throwable): Authorization = Authorization(errors(throwable))

  private def errors(throwable: Throwable): Seq[String] = Seq(throwable.getMessage)
}
