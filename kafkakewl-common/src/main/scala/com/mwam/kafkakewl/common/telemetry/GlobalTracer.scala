/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.telemetry

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.{DefaultTracer, Tracer}
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk
import zio.{TaskLayer, ZIO, ZLayer, System}

object GlobalTracer {
  val live: TaskLayer[Tracer] =
    ZLayer.fromZIO(
      {
        for {
          _ <- System.env("OTEL_EXPORTER_OTLP_ENDPOINT").flatMap(zio.ZIO.fromOption)
          _ <- System.env("OTEL_EXPORTER_OTLP_PROTOCOL").flatMap(zio.ZIO.fromOption)
          _ <- System.env("OTEL_SERVICE_NAME").flatMap(zio.ZIO.fromOption)
          _ <- System.env("OTEL_TRACES_EXPORTER").flatMap(zio.ZIO.fromOption)
        } yield AutoConfiguredOpenTelemetrySdk.initialize.getOpenTelemetrySdk.getTracer(getClass.getPackageName)
      }
        .orElse(ZIO.succeed(OpenTelemetry.noop().getTracer(getClass.getPackageName)))
    )
}
