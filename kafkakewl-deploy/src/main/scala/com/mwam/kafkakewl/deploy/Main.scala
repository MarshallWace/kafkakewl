/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy

import com.mwam.kafkakewl.common.http.HttpServer
import com.mwam.kafkakewl.common.persistence.{KafkaPersistentStore, PersistentStore}
import com.mwam.kafkakewl.common.telemetry.GlobalTracer
import com.mwam.kafkakewl.deploy.endpoints.*
import com.mwam.kafkakewl.deploy.services.{TopologyDeploymentsService, TopologyDeploymentsToKafkaService}
import com.mwam.kafkakewl.domain.config.KafkaClientConfig
import sttp.tapir.server.metrics.zio.ZioMetrics
import sttp.tapir.server.ziohttp.{ZioHttpInterpreter, ZioHttpServerOptions}
import zio.http.Server
import zio.metrics.connectors.prometheus
import zio.metrics.jvm.DefaultJvmMetrics
import zio.telemetry.opentelemetry.context.ContextStorage
import zio.telemetry.opentelemetry.tracing.Tracing
import com.mwam.kafkakewl.utils.logging.Logging.{deployLogger, localLogger}
import zio.*

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
      endpoints <- ZIO.service[Endpoints]
      httpApp = ZioHttpInterpreter(options).toHttp(endpoints.endpoints)
      actualPort <- Server.install(httpApp.withDefaultErrorResponse)
      _ <- ZIO.logInfo("kafkakewl-deploy started")
      _ <- ZIO.logInfo(s"api: http://localhost:$actualPort/api/v1")
      _ <- ZIO.logInfo(s"swagger: http://localhost:$actualPort/docs")
      // TODO currently ZIO.never does not handle SIGTERM and SIGINT/SIGKILL is needed to kill the app
      _ <- ZIO.never
    yield ()).provide(
      MainConfig.live,
      KafkaClusterConfig.live,
      KafkaClientConfig.live,
      HttpConfig.live,
      KafkaPersistentStoreConfig.live,
      MetricsConfig.live,
      KafkaPersistentStore.live,
      TopologyDeploymentsService.live,
      TopologyDeploymentsToKafkaService.live,
      DeploymentsEndpoints.live,
      DeploymentsServerEndpoints.live,
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
