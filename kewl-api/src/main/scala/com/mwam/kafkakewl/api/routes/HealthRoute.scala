/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.routes

import akka.actor.ActorRef
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.util.Timeout
import com.mwam.kafkakewl.domain.Command._
import com.mwam.kafkakewl.domain.CommandMetadata

import scala.concurrent.ExecutionContextExecutor

object HealthRoute extends RouteUtils {
  def route(processor: ActorRef)(implicit ec: ExecutionContextExecutor, timeout: Timeout): Route = {
    // the liveness check is done for a pathless GET request
    path("") {
      get {
        implicit val metricNames = MetricNames("get:")
        completeFuture((processor ? HealthIsLive(CommandMetadata("na"))).mapTo[HealthIsLiveResponse])
      }
    } ~
    pathPrefix("health") {
      pathPrefix("live") {
        get {
          implicit val metricNames = MetricNames("get:health_live")
          completeFuture((processor ? HealthIsLive(CommandMetadata("na"))).mapTo[HealthIsLiveResponse])
        }
      } ~
      pathPrefix("ready") {
        get {
          implicit val metricNames = MetricNames("get:health_ready")
          completeFuture((processor ? HealthIsReady(CommandMetadata("na"))).mapTo[HealthIsReadyResponse])
        }
      }
    }
  }
}
