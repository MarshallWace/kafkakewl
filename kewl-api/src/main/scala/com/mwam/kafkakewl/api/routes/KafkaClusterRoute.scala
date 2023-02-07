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
import com.mwam.kafkakewl.domain.kafkacluster.JsonEncodeDecode._
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.{Command, CommandError, CommandMetadata}
import com.mwam.kafkakewl.processor.state.StateReadOnlyCommandProcessor
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._

import scala.concurrent.ExecutionContextExecutor

object KafkaClusterRoute extends RouteUtils {
  def route(processor: ActorRef, readOnlyProcessor: StateReadOnlyCommandProcessor)(user: String)(implicit ec: ExecutionContextExecutor, timeout: Timeout): Route =
    pathPrefix("kafkacluster") {
      pathEnd {
        dryRunParam { dryRun =>
          compactParam { compact =>
            get {
              implicit val metricNames = MetricNames("get:kafkacluster_all")
              readOnlyProcessor ?? KafkaClusterGetAll(CommandMetadata(user, dryRun), compact)
            }
          } ~
          post {
            entity(as[KafkaCluster]) { kafkaCluster =>
              val kafkaClusterId = kafkaCluster.kafkaCluster
              implicit val metricNames = MetricNames("post:kafkacluster")
              val command = KafkaClusterCreate(CommandMetadata(user, dryRun), kafkaClusterId, kafkaCluster)
              if (kafkaCluster.kafkaCluster.isEmpty) {
                failure(command, CommandError.validationError(s"kafka-cluster id cannot be missing or empty"))
              } else {
                processor ?? command
              }
            }
          }
        }
      } ~
      path(Segment) { kafkaClusterIdString =>
        val kafkaClusterId = KafkaClusterEntityId(kafkaClusterIdString)
        def populateIdIfEmpty(kafkaCluster: KafkaCluster): KafkaCluster = kafkaCluster.copy(kafkaCluster = if (kafkaCluster.kafkaCluster.isEmpty) kafkaClusterId else kafkaCluster.kafkaCluster)
        def createOrUpdate(kafkaCluster: KafkaCluster, createCommand: KafkaCluster => Command)(implicit metricNames: MetricNames): StandardRoute = {
          val kafkaClusterWithId = populateIdIfEmpty(kafkaCluster)
          val command = createCommand(kafkaClusterWithId)
          if (kafkaClusterWithId.kafkaCluster != kafkaClusterId) {
            failure(command, CommandError.validationError(s"kafka-cluster id (${kafkaClusterWithId.kafkaCluster.quote}) cannot be different from the end-point (${kafkaClusterId.quote})"))
          } else {
            processor ?? command
          }
        }

        dryRunParam { dryRun =>
          compactParam { compact =>
            get {
              implicit val metricNames = MetricNames("get:kafkacluster")
              readOnlyProcessor ?? KafkaClusterGet(CommandMetadata(user, dryRun), kafkaClusterId, compact)
            }
          } ~
          post {
            entity(as[KafkaCluster]) { kafkaCluster =>
              implicit val metricNames = MetricNames("post:kafkacluster")
              createOrUpdate(kafkaCluster, KafkaClusterCreate(CommandMetadata(user, dryRun), kafkaClusterId, _))
            }
          } ~
          put {
            entity(as[KafkaCluster]) { kafkaCluster =>
              implicit val metricNames = MetricNames("put:kafkacluster")
              createOrUpdate(kafkaCluster, KafkaClusterUpdate(CommandMetadata(user, dryRun), kafkaClusterId, _))
            }
          } ~
          delete {
            implicit val metricNames = MetricNames("delete:kafkacluster")
            processor ?? KafkaClusterDelete(CommandMetadata(user, dryRun), kafkaClusterId)
          }
        }
      }
    }
}
