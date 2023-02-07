/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.routes

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.RouteDirectives
import akka.http.scaladsl.server.{Route, StandardRoute}
import cats.syntax.either._
import com.mwam.kafkakewl.api.metrics.services.KafkaClusterService
import com.mwam.kafkakewl.common.http.HttpExtensions._
import com.mwam.kafkakewl.domain.kafka.JsonEncodeDecode._
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.metrics.JsonEncodeDecode._
import com.mwam.kafkakewl.utils._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.Encoder

import scala.concurrent.ExecutionContextExecutor

object KafkaClusterRoute extends RouteUtils {
  private def complete[T : Encoder](metricNames: String*)(result: => KafkaClusterService.Result[T]): StandardRoute = {
    // it's a function so that it's evaluated ONLY when RouteDirectives.complete() evaluates the response by name parameter,
    // so this whole thing becomes lazy
    def response: ToResponseMarshallable = {
      val (evaluatedResult, duration) = durationOf { result }
      metricNames.foreach(metricName => metrics.timer(s"$metricName:duration:all").update(duration))
      metricNames.foreach(metricName => metrics.meter(s"$metricName:meter:all").mark())
      evaluatedResult match {
        case Left(error) =>
          metricNames.foreach(metricName => metrics.timer(s"$metricName:duration:failure").update(duration))
          metricNames.foreach(metricName => metrics.meter(s"$metricName:meter:failure").mark())
          responseAsJsonWithStatusCode(error, StatusCodes.InternalServerError)
        case Right(r) =>
          metricNames.foreach(metricName => metrics.timer(s"$metricName:duration:success").update(duration))
          metricNames.foreach(metricName => metrics.meter(s"$metricName:meter:success").mark())
          responseAsJsonWithStatusCode(r)
      }
    }
    RouteDirectives.complete(response)
  }

  def route(service: KafkaClusterService)(user: String)(implicit ec: ExecutionContextExecutor): Route = {
    pathEndOrSingleSlash {
      get {
        complete(s"kafkaclusterids:all") {
          service.getKafkaClusterIds.map(_.map(_.id))
        }
      }
    } ~
    pathPrefix(Segment) { kid =>
      val kafkaClusterId = KafkaClusterEntityId(kid)
      path("topic") {
        get {
          entity(as[List[String]]) { topicNames =>
            complete(s"kafkaclustertopics:$kid") {
              service.getTopicsInfo(kafkaClusterId, topicNames).asRight
            }
          } ~
          complete(s"kafkaclustertopics:$kid", s"kafkaclusteralltopics:$kid") {
            service.getTopics(kafkaClusterId)
          }
        }
      } ~
      path("topic" / Segment) { topic =>
        get {
          complete(s"kafkaclustertopic:$kid") {
            service.getTopicInfo(kafkaClusterId, topic)
          }
        }
      } ~
      path("group") {
        get {
          entity(as[List[String]]) { groups =>
            complete(s"kafkaclustergroups:$kid") {
              service.getConsumerGroupsMetrics(kafkaClusterId, groups).asRight
            }
          } ~
          complete(s"kafkaclustergroups:$kid", s"kafkaclusterallgroups:$kid") {
            service.getConsumerGroups(kafkaClusterId)
          }
        }
      } ~
      path("group" / Segment) { group =>
        get {
          complete(s"kafkaclustergroup:$kid") {
            service.getConsumerGroupMetrics(kafkaClusterId, group)
          }
        }
      } ~
      path("group" / Segment / "topic" / Segment) { (group, topic) =>
        get {
          complete(s"kafkaclustergroup:$kid", s"kafkaclustertopic:$kid", s"kafkaclustergrouptopic:$kid") {
            service.getConsumerGroupTopicMetrics(kafkaClusterId, group, topic)
          }
        }
      }
    }
  }
}
