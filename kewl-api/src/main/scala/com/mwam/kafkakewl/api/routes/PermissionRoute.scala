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
import com.mwam.kafkakewl.domain.permission.JsonEncodeDecode._
import com.mwam.kafkakewl.domain.permission.{Permission, PermissionEntityId}
import com.mwam.kafkakewl.processor.state.StateReadOnlyCommandProcessor
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._

import scala.concurrent.ExecutionContextExecutor

object PermissionRoute extends RouteUtils {
  def route(processor: ActorRef, readOnlyProcessor: StateReadOnlyCommandProcessor)(user: String)(implicit ec: ExecutionContextExecutor, timeout: Timeout): Route =
    pathPrefix("permission") {
      pathEnd {
        dryRunParam { dryRun =>
          compactParam { compact =>
            get {
              implicit val metricNames = MetricNames("get:permission_all")
              readOnlyProcessor ?? PermissionGetAll(CommandMetadata(user, dryRun), compact)
            }
          } ~
          post {
            entity(as[Permission]) { permission =>
              implicit val metricNames = MetricNames("post:permission")
              processor ?? PermissionCreateWithContent(CommandMetadata(user, dryRun), permission)
            }
          } ~
          delete {
            entity(as[Permission]) { permission =>
              implicit val metricNames = MetricNames("delete:permission")
              processor ?? PermissionDeleteByContent(CommandMetadata(user, dryRun), permission)
            }
          }
        }
      } ~
      path(Segment) { permissionIdString =>
        val permissionId = PermissionEntityId(permissionIdString)
        dryRunParam { dryRun =>
          compactParam { compact =>
            get {
              implicit val metricNames = MetricNames("get:permission_id")
              readOnlyProcessor ?? PermissionGet(CommandMetadata(user, dryRun), permissionId, compact)
            }
          } ~
          post {
            entity(as[Permission]) { permission =>
              implicit val metricNames = MetricNames("post:permission_id")
              processor ?? PermissionCreate(CommandMetadata(user, dryRun), permissionId, permission)
            }
          } ~
          put {
            entity(as[Permission]) { permission =>
              implicit val metricNames = MetricNames("put:permission_id")
              processor ?? PermissionUpdate(CommandMetadata(user, dryRun), permissionId, permission)
            }
          } ~
          delete {
            implicit val metricNames = MetricNames("delete:permission_id")
            processor ?? PermissionDelete(CommandMetadata(user, dryRun), permissionId)
          }
        }
      }
    }
}
