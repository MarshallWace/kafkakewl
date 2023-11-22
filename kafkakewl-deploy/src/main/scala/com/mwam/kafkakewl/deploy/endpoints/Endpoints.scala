/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy.endpoints

import sttp.tapir.PublicEndpoint
import sttp.tapir.swagger.bundle.SwaggerInterpreter
import sttp.tapir.ztapir.*
import zio.*
import zio.metrics.connectors.timelessprometheus.TimelessPrometheusPublisher

class Endpoints(
    deploymentServerEndpoints: DeploymentsServerEndpoints,
    prometheusPublisher: TimelessPrometheusPublisher
) {
  private val metricsEndpoint: PublicEndpoint[Unit, Unit, String, Any] =
    endpoint.in("metrics").get.out(stringBody)

  // Health check endpoints. As of now, just return 200 no preparation is done
  // after the HTTP server starts.
  private val livenessEndpoint: PublicEndpoint[Unit, Nothing, Unit, Any] =
    infallibleEndpoint.in("health").in("live").get
  private val readinessEndpoint: PublicEndpoint[Unit, Nothing, Unit, Any] =
    infallibleEndpoint.in("health").in("ready").get
  private val startupProbeEndpoint: PublicEndpoint[Unit, Nothing, Unit, Any] =
    infallibleEndpoint.in("health").in("startup").get

  val endpoints: List[ZServerEndpoint[Any, Any]] = {
    val api = deploymentServerEndpoints.endpoints
    val docs = docsEndpoints(api)
    val metrics = List(metricsEndpoint.zServerLogic[Any](_ => getMetrics))
    val health =
      List(livenessEndpoint, readinessEndpoint, startupProbeEndpoint)
        .map(
          _.zServerLogic[Any](_ => ZIO.succeed(()))
        )
    api ++ docs ++ metrics ++ health
  }

  private def docsEndpoints(
      apiEndpoints: List[ZServerEndpoint[Any, Any]]
  ): List[ZServerEndpoint[Any, Any]] = SwaggerInterpreter()
    .fromServerEndpoints[Task](apiEndpoints, "kafkakewl-deploy", "1.0.0")

  private def getMetrics: ZIO[Any, Unit, String] =
    prometheusPublisher.get // TODO this adds the timestamp as well which isn't desirable due to prometheus's staleness handling

}

object Endpoints {
  val live: ZLayer[
    DeploymentsServerEndpoints & TimelessPrometheusPublisher,
    Nothing,
    Endpoints
  ] =
    ZLayer.fromFunction(Endpoints(_, _))
}
