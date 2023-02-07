/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.mwam.kafkakewl.api.metrics.services.KafkaClusterService

import scala.concurrent.ExecutionContextExecutor

object HealthRoute extends RouteUtils {
  def route(service: KafkaClusterService)(implicit ec: ExecutionContextExecutor, timeout: Timeout): Route =
    pathPrefix("health") {
      pathPrefix("live") {
        get {
          // dummy call just to make sure it doesn't fail with anything
          val _ = service.getKafkaClusterIds
          complete("live")
        }
      } ~
      pathPrefix("ready") {
        get {
          // dummy call just to make sure it doesn't fail with anything
          val _ = service.getKafkaClusterIds
          complete("ready")
        }
      }
    }
}
