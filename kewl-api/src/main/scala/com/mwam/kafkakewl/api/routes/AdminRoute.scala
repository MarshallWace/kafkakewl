/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.routes

import akka.actor.ActorRef
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import com.mwam.kafkakewl.domain.Command._
import com.mwam.kafkakewl.domain.CommandMetadata
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.utils._

import scala.concurrent.ExecutionContextExecutor

object AdminRoute extends RouteUtils {
  def route(processor: ActorRef)(user: String)(implicit ec: ExecutionContextExecutor, timeout: Timeout): Route =
    pathPrefix("admin") {
      pathPrefix("plugin") {
        pathPrefix("permission") {
          get {
            path(Segment) { userName =>
              implicit val metricNames = MetricNames("get:admin_plugin_permissions")
              processor ?? PermissionPluginGetUserPermissions(CommandMetadata(user), userName.trim.toLowerCase)
            }
          } ~
          post {
            pathPrefix("invalidatecached") {
              pathEnd {
                implicit val metricNames = MetricNames("post:admin_plugin_permissions_invalidatecached_all")
                processor ?? PermissionPluginInvalidateCachedUserPermissions(CommandMetadata(user), None)
              } ~
              path(Segment) { userName =>
                implicit val metricNames = MetricNames("post:admin_plugin_permissions_invalidatecached_user")
                processor ?? PermissionPluginInvalidateCachedUserPermissions(CommandMetadata(user), Option(userName).map(_.trim.toLowerCase).flatMap(_.noneIfEmpty))
              }
            }
          }
        }
      } ~
      path("reinit") {
        post {
          parameters("wipe" ? "false") { wipeString =>
            val wipe = wipeString.toBooleanOrNone.getOrElse(false)
            // no dry-run here, no point
            implicit val metricNames = if (wipe) MetricNames("post:admin_reinit_wipe") else MetricNames("post:admin_reinit")
            processor ?? ReInitialize(CommandMetadata(user), wipe)
          }
        }
      } ~
      path("kafka" / Segment / "diff") { kafkaClusterIdString =>
        val kafkaClusterId = KafkaClusterEntityId(kafkaClusterIdString)
        get {
          implicit val metricNames = MetricNames("get:admin_kafka_diff")
          processor ?? KafkaDiff(CommandMetadata(user), kafkaClusterId, ignoreNonKewlKafkaResources = true)
        }
      } ~
      path("kafka" / Segment / "diff_all") { kafkaClusterIdString =>
        val kafkaClusterId = KafkaClusterEntityId(kafkaClusterIdString)
        get {
          implicit val metricNames = MetricNames("get:admin_kafka_diff_all")
          processor ?? KafkaDiff(CommandMetadata(user), kafkaClusterId, ignoreNonKewlKafkaResources = false)
        }
      }
    }
}
