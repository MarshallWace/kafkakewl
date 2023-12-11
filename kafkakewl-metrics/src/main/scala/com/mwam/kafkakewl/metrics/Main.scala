/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics

import com.mwam.kafkakewl.common.http.HttpServer
import com.mwam.kafkakewl.common.kafka.KafkaConsumer
import com.mwam.kafkakewl.common.telemetry.GlobalTracer
import com.mwam.kafkakewl.domain.config.KafkaClientConfig
import com.mwam.kafkakewl.metrics.endpoints.*
import com.mwam.kafkakewl.metrics.services.*
import com.mwam.kafkakewl.utils.logging.Logging.{deployLogger, localLogger}
import io.opentelemetry.api.trace.Tracer
import sttp.tapir.server.metrics.zio.ZioMetrics
import sttp.tapir.server.ziohttp.{ZioHttpInterpreter, ZioHttpServerOptions}
import zio.*
import zio.http.Server
import zio.metrics.connectors.prometheus
import zio.metrics.jvm.DefaultJvmMetrics
import zio.telemetry.opentelemetry.context.ContextStorage
import zio.telemetry.opentelemetry.tracing.Tracing

object Main extends ZIOAppDefault {

  override val bootstrap: ZLayer[ZIOAppArgs, Any, Any] =
    ZLayer.fromZIO {
      ZIOAppArgs.getArgs
        .map(args =>
          if (args.mkString == "local") {
            localLogger
          } else {
            deployLogger
          }
        )
    }.flatten

  override def run: ZIO[ZIOAppArgs with Scope, Any, Any] = {
    val options: ZioHttpServerOptions[Any] = ZioHttpServerOptions.customiseInterceptors
      .metricsInterceptor(ZioMetrics.default[Task]().metricsInterceptor())
      //      .exceptionHandler(new DefectHandler())
      //      .corsInterceptor(
      //        CORSInterceptor.customOrThrow(
      //          CORSConfig.default.copy(
      //            allowedOrigin = AllowedOrigin.All
      //          )
      //        )
      //      )
      //      .decodeFailureHandler(CustomDecodeFailureHandler.create())
      .options

    (for
      topicInfoSource <- ZIO.service[KafkaTopicInfoSource]
      _ <- topicInfoSource.startPublishing()
      consumerOffsetsSource <- ZIO.service[ConsumerOffsetsSource]
      _ <- consumerOffsetsSource.startPublishing()

      endpoints <- ZIO.service[Endpoints]
      httpApp = ZioHttpInterpreter(options).toHttp(endpoints.endpoints)
      actualPort <- Server.install(httpApp.withDefaultErrorResponse)
      _ <- ZIO.logInfo("kafkakewl-metrics started")
      _ <- ZIO.logInfo(s"api: http://localhost:$actualPort/api/v1")
      _ <- ZIO.logInfo(s"swagger: http://localhost:$actualPort/docs")
      // TODO currently ZIO.never does not handle SIGTERM and SIGINT/SIGKILL is needed to kill the app
      _ <- ZIO.never
    yield ()).provide(
      MainConfig.live,
      KafkaClusterConfig.live,
      ConsumerOffsetsSourceConfig.live,
      KafkaClientConfig.live,
      HttpConfig.live,
      MetricsConfig.live,
      KafkaConsumer.live,
      ConsumerOffsetsSource.live,
      KafkaTopicInfoSource.live,
      KafkaTopicInfoCache.live,
      KafkaConsumerGroupInfoCache.live,
      TopicEndpoints.live,
      TopicServerEndpoints.live,
      ConsumerGroupEndpoints.live,
      ConsumerGroupServerEndpoints.live,
      Endpoints.live,
      prometheus.publisherLayer,
      prometheus.prometheusLayer,
      Runtime.enableRuntimeMetrics,
      DefaultJvmMetrics.live.unit,
      HttpServer.live,
      Tracing.live,
      GlobalTracer.live,
      ContextStorage.openTelemetryContext
    )
  }
}
