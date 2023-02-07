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
import com.mwam.kafkakewl.domain.topology._
import com.mwam.kafkakewl.processor.state.StateReadOnlyCommandProcessor

import scala.concurrent.ExecutionContextExecutor

object DeployedTopologyMetricsRoute extends RouteUtils {
  def route(processor: ActorRef, readOnlyProcessor: StateReadOnlyCommandProcessor)(user: String)(implicit ec: ExecutionContextExecutor, timeout: Timeout): Route =
    pathPrefix("metrics" / "deployedtopology") {
      dryRunParam { dryRun =>
        pathEnd {
          compactParam { compact =>
            get {
              implicit val metricNames = MetricNames("get:metrics_deployedtopology_all")
              readOnlyProcessor ?? DeployedTopologyMetricsGetKafkaClustersAll(CommandMetadata(user, dryRun), compact)
            }
          }
        } ~
        pathPrefix(Segment) { kafkaClusterIdString =>
          val kafkaClusterId = KafkaClusterEntityId(kafkaClusterIdString)
          pathEnd {
            dryRunParam { dryRun =>
              compactParam { compact =>
                get {
                  implicit val metricNames = MetricNames("get:metrics_deployedtopology_kafkacluster")
                  readOnlyProcessor ?? DeployedTopologyMetricsGetAll(CommandMetadata(user, dryRun), kafkaClusterId, compact)
                }
              }
            }
          } ~
          pathPrefix(Segment) { topologyIdString =>
            val topologyId = TopologyEntityId(topologyIdString)
            dryRunParam { dryRun =>
              pathEnd {
                compactParam { compact =>
                  get {
                    implicit val metricNames = MetricNames("get:metrics_deployedtopology_kafkacluster_topology")
                    readOnlyProcessor ?? DeployedTopologyMetricsGet(CommandMetadata(user, dryRun), kafkaClusterId, topologyId, compact)
                  }
                }
              }
            }
          }
        }
      }
    }
}
