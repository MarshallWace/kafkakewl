/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy.endpoints

import com.mwam.kafkakewl.common.http.{EndpointUtils, ErrorResponse}
//import com.mwam.kafkakewl.deploy.domain.{Failures, QueryFailure}
import sttp.model.StatusCode
import sttp.tapir.{EndpointOutput, PublicEndpoint}
import sttp.tapir.swagger.bundle.SwaggerInterpreter
import sttp.tapir.ztapir.*
//import sttp.tapir.ztapir.a
import zio.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.zio.jsonBody
import zio.metrics.connectors.prometheus.PrometheusPublisher

import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder}

class Endpoints(
    deploymentServerEndpoints: DeploymentsServerEndpoints,
    prometheusPublisher: PrometheusPublisher
) {
  private val metricsEndpoint: PublicEndpoint[Unit, Unit, String, Any] =
    endpoint
      .in("metrics")
      .get
      .out(stringBody)

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
    val metrics = List(
      metricsEndpoint.zServerLogic[Any](_ => getMetrics)
    )
    val health =
      List(livenessEndpoint, readinessEndpoint, startupProbeEndpoint)
        .map(
          _.zServerLogic[Any](_ => ZIO.succeed(()))
        )
    api ++ docs ++ metrics ++ health
  }

  private def docsEndpoints(apiEndpoints: List[ZServerEndpoint[Any, Any]]): List[ZServerEndpoint[Any, Any]] = SwaggerInterpreter()
    .fromServerEndpoints[Task](apiEndpoints, "kafkakewl-deploy", "1.0.0")

  // Regex to remove timestamp from the end of each metric. This is to fix the staleness issue in prometheus.
  private def getMetrics: ZIO[Any, Unit, String] =
    prometheusPublisher.get.map(_.replaceAll("[0-9]+\n", "\n"))
}

object Endpoints {
  val live: ZLayer[DeploymentsServerEndpoints & PrometheusPublisher, Nothing, Endpoints] =
    ZLayer.fromFunction(Endpoints(_, _))
}
