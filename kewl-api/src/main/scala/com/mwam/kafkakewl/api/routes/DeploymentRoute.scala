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
import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.domain.deploy.JsonEncodeDecode._
import com.mwam.kafkakewl.domain.deploy.{DeploymentChange, DeploymentOptions}
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.topology.TopologyEntityId
import com.mwam.kafkakewl.processor.state.StateReadOnlyCommandProcessor
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._

import scala.concurrent.ExecutionContextExecutor

object DeploymentRoute extends RouteUtils {
  def route(processor: ActorRef, readOnlyProcessor: StateReadOnlyCommandProcessor)(user: String)(implicit ec: ExecutionContextExecutor, timeout: Timeout): Route =
    pathPrefix("deployment") {
      pathEnd {
        dryRunParam { dryRun =>
          compactParam { compact =>
            get {
              implicit val metricNames = MetricNames("get:deployment_all")
              readOnlyProcessor ?? DeploymentGetAll(CommandMetadata(user, dryRun), compact)
            }
          }
        }
      } ~
      pathPrefix(Segment / Segment) { (kafkaClusterIdString, topologyIdString) =>
        val kafkaClusterId = KafkaClusterEntityId(kafkaClusterIdString)
        val topologyId = TopologyEntityId(topologyIdString)
        // custom commands for deployments
        path("reapply") {
          dryRunParam { dryRun =>
            post {
              entity(as[DeploymentOptions]) { options =>
                implicit val metricNames = MetricNames("post:deployment_kafkacluster_topology_reapply_options")
                processor ?? DeploymentReApply(CommandMetadata(user, dryRun), kafkaClusterId, topologyId, options)
              }
            } ~
            post {
              dryRunParam { dryRun =>
                implicit val metricNames = MetricNames("post:deployment_kafkacluster_topology_reapply")
                processor ?? DeploymentReApply(CommandMetadata(user, dryRun), kafkaClusterId, topologyId)
              }
            }
          }
        } ~
        // the usual GET/POST/PUT/DELETE commands
        pathEnd {
          def populateDeployment(deploymentChange: DeploymentChange): DeploymentChange = {
            deploymentChange.copy(
              kafkaClusterId = if (deploymentChange.kafkaClusterId.isEmpty) kafkaClusterId else deploymentChange.kafkaClusterId,
              topologyId = if (deploymentChange.topologyId.isEmpty) topologyId else deploymentChange.topologyId
            )
          }

          dryRunParam { dryRun =>
            compactParam { compact =>
              get {
                implicit val metricNames = MetricNames("get:deployment_kafkacluster_topology")
                readOnlyProcessor ?? DeploymentGet(CommandMetadata(user, dryRun), kafkaClusterId, topologyId, compact)
              }
            } ~
            post {
              entity(as[DeploymentChange]) { deployment =>
                implicit val metricNames = MetricNames("post:deployment_kafkacluster_topology")
                processor ?? DeploymentCreate(CommandMetadata(user, dryRun), kafkaClusterId, topologyId, populateDeployment(deployment))
              }
            } ~
            put {
              entity(as[DeploymentChange]) { deployment =>
                implicit val metricNames = MetricNames("put:deployment_kafkacluster_topology")
                processor ?? DeploymentUpdate(CommandMetadata(user, dryRun), kafkaClusterId, topologyId, populateDeployment(deployment))
              }
            } ~
            delete {
              implicit val metricNames = MetricNames("delete:deployment_kafkacluster_topology")
              processor ?? DeploymentDelete(CommandMetadata(user, dryRun), kafkaClusterId, topologyId)
            }
          }
        }
      }
    }
}
