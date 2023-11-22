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
import zio.metrics.connectors.prometheus.PrometheusPublisher

class Endpoints(
    deploymentServerEndpoints: DeploymentsServerEndpoints,
    prometheusPublisher: PrometheusPublisher
) {
  private val metricsEndpoint: PublicEndpoint[Unit, Unit, String, Any] =
    endpoint.in("metrics").get.out(stringBody)

  val endpoints: List[ZServerEndpoint[Any, Any]] = {
    val api = deploymentServerEndpoints.endpoints
    val docs = docsEndpoints(api)
    val metrics = List(metricsEndpoint.zServerLogic[Any](_ => getMetrics))
    api ++ docs ++ metrics
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
    DeploymentsServerEndpoints & PrometheusPublisher,
    Nothing,
    Endpoints
  ] =
    ZLayer.fromFunction(Endpoints(_, _))
}
