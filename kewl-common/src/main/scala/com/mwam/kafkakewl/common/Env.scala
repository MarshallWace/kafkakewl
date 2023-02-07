/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

sealed trait Env
object Env {
  final case object Dev extends Env
  final case object Prod extends Env

  def apply(e: String): Option[Env] = {
    e.toLowerCase match {
      case "dev" => Some(Dev)
      case "prod" => Some(Prod)
      case _ => None
    }
  }
}
