/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.endpoints

import sttp.tapir.PublicEndpoint
import sttp.tapir.swagger.bundle.SwaggerInterpreter
import sttp.tapir.ztapir.*
import zio.*
import zio.metrics.connectors.prometheus.PrometheusPublisher

class Endpoints(
    topicEndpoints: TopicServerEndpoints,
    consumerGroupEndpoints: ConsumerGroupServerEndpoints,
    prometheusPublisher: PrometheusPublisher
) {
  private val metricsEndpoint: PublicEndpoint[Unit, Unit, String, Any] = endpoint.in("metrics").get.out(stringBody)

  // Health check endpoints. As of now, just return 200 no preparation is done
  // after the HTTP server starts.
  private val livenessEndpoint: PublicEndpoint[Unit, Nothing, Unit, Any] =
    infallibleEndpoint.in("health").in("live").get
  private val readinessEndpoint: PublicEndpoint[Unit, Nothing, Unit, Any] =
    infallibleEndpoint.in("health").in("ready").get
  private val startupProbeEndpoint: PublicEndpoint[Unit, Nothing, Unit, Any] =
    infallibleEndpoint.in("health").in("startup").get

  val endpoints: List[ZServerEndpoint[Any, Any]] = {
    val api = topicEndpoints.endpoints ++ consumerGroupEndpoints.endpoints
    val docs = docsEndpoints(api)
    val metrics = List(metricsEndpoint.zServerLogic[Any](_ => getMetrics))
    val health =
      List(livenessEndpoint, readinessEndpoint, startupProbeEndpoint)
        .map(
          _.zServerLogic[Any](_ => ZIO.succeed(()))
        )
    api ++ docs ++ metrics ++ health
  }

  private def docsEndpoints(apiEndpoints: List[ZServerEndpoint[Any, Any]]): List[ZServerEndpoint[Any, Any]] = SwaggerInterpreter()
    .fromServerEndpoints[Task](apiEndpoints, "kafkakewl-metrics", "1.0.0")

  // Regex to remove timestamp from the end of each metric. This is to fix the staleness issue in prometheus.
  private def getMetrics: ZIO[Any, Unit, String] =
    prometheusPublisher.get.map(_.replaceAll("[0-9]+\n", "\n"))
}

object Endpoints {
  val live: ZLayer[TopicServerEndpoints & ConsumerGroupServerEndpoints & PrometheusPublisher, Nothing, Endpoints] =
    ZLayer.fromFunction(Endpoints(_, _, _))
}
