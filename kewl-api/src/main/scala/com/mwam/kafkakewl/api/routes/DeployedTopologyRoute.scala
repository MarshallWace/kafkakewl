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
import com.mwam.kafkakewl.domain.deploy.DeployedTopology.ResetOptions
import com.mwam.kafkakewl.domain.deploy.JsonEncodeDecode._
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.topology._
import com.mwam.kafkakewl.processor.state.StateReadOnlyCommandProcessor
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._

import scala.concurrent.ExecutionContextExecutor

object DeployedTopologyRoute extends RouteUtils {
  def route(processor: ActorRef, readOnlyProcessor: StateReadOnlyCommandProcessor)(user: String)(implicit ec: ExecutionContextExecutor, timeout: Timeout): Route =
    pathPrefix("deployedtopology") {
      dryRunParam { dryRun =>
        pathEnd {
          compactParam { compact =>
            get {
              implicit val metricNames = MetricNames("get:deployedtopology_all")
              readOnlyProcessor ?? DeployedTopologyGetKafkaClustersAll(CommandMetadata(user, dryRun), compact)
            }
          }
        }
      } ~
      pathPrefix(Segment) { kafkaClusterIdString =>
        val kafkaClusterId = KafkaClusterEntityId(kafkaClusterIdString)
        pathEnd {
          dryRunParam { dryRun =>
            compactParam { compact =>
              get {
                implicit val metricNames = MetricNames("get:deployedtopology_kafkacluster")
                readOnlyProcessor ?? DeployedTopologyGetAll(CommandMetadata(user, dryRun), kafkaClusterId, compact)
              }
            }
          }
        } ~
        pathPrefix(Segment) { topologyIdString =>
          val topologyId = TopologyEntityId(topologyIdString)
          dryRunParam { dryRun =>
            path("reset") {
              post {
                entity(as[ResetOptions]) { options =>
                  implicit val metricNames = MetricNames("post:deployedtopology_kafkacluster_topology_reset")
                  processor ?? DeployedTopologyReset(CommandMetadata(user, dryRun), kafkaClusterId, topologyId, options)
                }
              }
            } ~
            pathEnd {
              compactParam { compact =>
                get {
                  implicit val metricNames = MetricNames("get:deployedtopology_kafkacluster_topology")
                  readOnlyProcessor ?? DeployedTopologyGet(CommandMetadata(user, dryRun), kafkaClusterId, topologyId, compact)
                }
              }
            }
          } ~
          path("topic") {
            get {
              implicit val metricNames = MetricNames("get:deployedtopology_kafkacluster_topology_topic_all")
              readOnlyProcessor ?? DeployedTopologyTopicGetAll(CommandMetadata(user), kafkaClusterId, topologyId)
            }
          } ~
          path("topic" / Segment) { topicIdString =>
            val topicId = LocalTopicId(topicIdString)
            get {
              implicit val metricNames = MetricNames("get:deployedtopology_kafkacluster_topology_topic")
              processor ?? DeployedTopologyTopicGet(CommandMetadata(user), kafkaClusterId, topologyId, topicId)
            }
          } ~
          path("application") {
            get {
              implicit val metricNames = MetricNames("get:deployedtopology_kafkacluster_topology_application_all")
              readOnlyProcessor ?? DeployedTopologyApplicationGetAll(CommandMetadata(user), kafkaClusterId, topologyId)
            }
          } ~
          path("application" / Segment) { applicationIdString =>
            val applicationId = LocalApplicationId(applicationIdString)
            get {
              implicit val metricNames = MetricNames("get:deployedtopology_kafkacluster_topology_application")
              processor ?? DeployedTopologyApplicationGet(CommandMetadata(user), kafkaClusterId, topologyId, applicationId)
            }
          }
        }
      }
    }
}
