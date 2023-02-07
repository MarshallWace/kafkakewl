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
import com.mwam.kafkakewl.domain.JsonEncodeDecodeBase._
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.{CommandMetadata, FlexibleName}
import com.mwam.kafkakewl.processor.state.StateReadOnlyCommandProcessor
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._

import scala.concurrent.ExecutionContextExecutor

object ResolvedDeployedTopologyRoute extends RouteUtils {
  def route(processor: ActorRef, readOnlyProcessor: StateReadOnlyCommandProcessor)(user: String)(implicit ec: ExecutionContextExecutor, timeout: Timeout): Route =
    pathPrefix("resolved" / "deployedtopology") {
      pathPrefix(Segment) { kafkaClusterIdString =>
        val kafkaClusterId = KafkaClusterEntityId(kafkaClusterIdString)
        pathEnd {
          dryRunParam { dryRun =>
            get {
              entity(as[Seq[FlexibleName]]) { topologies =>
                implicit val metricNames = MetricNames("get:resolved_deployedtopology")
                readOnlyProcessor ?? DeployedTopologiesGetResolved(CommandMetadata(user, dryRun), kafkaClusterId, topologies)
              }
            } ~
            get {
              implicit val metricNames = MetricNames("get:resolved_deployedtopology_all")
              readOnlyProcessor ?? DeployedTopologiesGetResolved(CommandMetadata(user, dryRun), kafkaClusterId, Seq(FlexibleName.Any()))
            }
          }
        } ~
        pathPrefix(Segment) { topologyIdString =>
          dryRunParam { dryRun =>
            pathEnd {
              get {
                implicit val metricNames = MetricNames("get:resolved_deployedtopology_topology")
                readOnlyProcessor ?? DeployedTopologiesGetResolved(CommandMetadata(user, dryRun), kafkaClusterId, Seq(FlexibleName.Exact(topologyIdString)))
              }
            }
          }
        }
      }
    }
}
