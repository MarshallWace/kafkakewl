/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.http

import sttp.model.StatusCode
import sttp.tapir.*
import sttp.tapir.EndpointIO.annotations.*

trait EndpointUtils {
  val apiEndpoint: PublicEndpoint[Unit, Unit, Unit, Any] = endpoint.in("api" / "v1")
}

case class ErrorResponse(@jsonbody message: String, @statusCode statusCode: StatusCode)
