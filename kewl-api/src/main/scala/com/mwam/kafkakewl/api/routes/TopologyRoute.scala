/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.routes

import akka.actor.ActorRef
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Route, StandardRoute}
import akka.util.Timeout
import com.mwam.kafkakewl.domain.Command._
import com.mwam.kafkakewl.domain.topology.JsonEncodeDecode._
import com.mwam.kafkakewl.domain.topology.{Topology, TopologyEntityId}
import com.mwam.kafkakewl.domain.{Command, CommandError, CommandMetadata}
import com.mwam.kafkakewl.processor.state.StateReadOnlyCommandProcessor
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._

import scala.concurrent.ExecutionContextExecutor

object TopologyRoute extends RouteUtils {
  def route(processor: ActorRef, readOnlyProcessor: StateReadOnlyCommandProcessor)(user: String)(implicit ec: ExecutionContextExecutor, timeout: Timeout): Route =
    pathPrefix("topology") {
      pathEnd {
        dryRunParam { dryRun =>
          compactParam { compact =>
            get {
              implicit val metricNames = MetricNames("get:topology_all")
              readOnlyProcessor ?? TopologyGetAll(CommandMetadata(user, dryRun), compact)
            }
          } ~
          post {
            entity(as[Topology]) { topology =>
              val topologyId = topology.topologyEntityId
              implicit val metricNames = MetricNames("post:topology")
              val command = TopologyCreate(CommandMetadata(user, dryRun), topologyId, topology)
              if (topologyId.isEmpty) {
                failure(command, CommandError.validationError(s"at least one of the namespace or topology must be specified"))
              } else {
                processor ?? command
              }
            }
          }
        }
      } ~
      path(Segment) { topologyIdString =>
        val topologyId = TopologyEntityId(topologyIdString)
        def createOrUpdate(topology: Topology, command: Command)(implicit metricNames: MetricNames): StandardRoute = {
          if (topology.topologyEntityId != topologyId) {
            failure(command, CommandError.validationError(s"the topology's id (${topology.topologyEntityId.quote}) cannot be different from the end-point (${topologyId.quote})"))
          } else {
            processor ?? command
          }
        }

        dryRunParam { dryRun =>
          compactParam { compact =>
            get {
              implicit val metricNames = MetricNames("get:topology")
              readOnlyProcessor ?? TopologyGet(CommandMetadata(user, dryRun), topologyId, compact)
            }
          }~
          post {
            entity(as[Topology]) { topology =>
              implicit val metricNames = MetricNames("post:topology")
              createOrUpdate(topology, TopologyCreate(CommandMetadata(user, dryRun), topologyId, topology))
            }
          } ~
          put {
            entity(as[Topology]) { topology =>
              implicit val metricNames = MetricNames("put:topology")
              createOrUpdate(topology, TopologyUpdate(CommandMetadata(user, dryRun), topologyId, topology))
            }
          } ~
          delete {
            implicit val metricNames = MetricNames("delete:topology")
            processor ?? TopologyDelete(CommandMetadata(user, dryRun), topologyId)
          }
        }
      } ~
      path(Segment / IntNumber) { (topologyIdString, topologyVersion) =>
        val topologyId = TopologyEntityId(topologyIdString)
        dryRunParam { dryRun =>
          compactParam { compact =>
            get {
              implicit val metricNames = MetricNames("get:topology_version")
              processor ?? TopologyGet(CommandMetadata(user, dryRun), topologyId, compact, Some(topologyVersion))
            }
          }
        }
      }
    }
}
