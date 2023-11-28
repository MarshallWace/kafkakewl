/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.telemetry

import sttp.tapir.*
import sttp.tapir.ztapir.*
import zio.ZIO
import zio.telemetry.opentelemetry.tracing.Tracing

extension [SECURITY_INPUT, INPUT, ERROR_OUTPUT, OUTPUT, C](e: Endpoint[SECURITY_INPUT, INPUT, ERROR_OUTPUT, OUTPUT, C]) {
  def zServerLogicWithTracing[R](
      logic: INPUT => ZIO[R, ERROR_OUTPUT, OUTPUT]
  )(using aIsUnit: SECURITY_INPUT =:= Unit, tracing: Tracing): ZServerEndpoint[R, C] = {
    import tracing.aspects.*
    e.zServerLogic(input => logic(input) @@ root(e.showShort))
  }
}
