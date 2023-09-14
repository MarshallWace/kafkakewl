/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.telemetry

import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk
import zio.{TaskLayer, ZIO, ZLayer}

object GlobalTracer {
  val live: TaskLayer[Tracer] =
    ZLayer.fromZIO(
      ZIO.attempt(AutoConfiguredOpenTelemetrySdk.initialize.getOpenTelemetrySdk.getTracer(getClass.getPackageName))
    )
}
