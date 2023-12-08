/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.endpoints

import com.mwam.kafkakewl.common.http.EndpointUtils
import com.mwam.kafkakewl.common.http.EndpointUtils.timeout
import com.mwam.kafkakewl.common.telemetry.zServerLogicWithTracing
import com.mwam.kafkakewl.domain.{TimeoutException, config}
import com.mwam.kafkakewl.metrics.domain.{Failures, KafkaSingleTopicPartitionInfos, QueryFailure}
import com.mwam.kafkakewl.metrics.services.KafkaTopicInfoCache
import com.mwam.kafkakewl.metrics.MainConfig
import sttp.tapir.ztapir.*
import zio.*
import zio.telemetry.opentelemetry.tracing.Tracing

class TopicServerEndpoints(topicEndpoints: TopicEndpoints, topicService: KafkaTopicInfoCache, tracing: Tracing, mainConfig: MainConfig) {
  given Tracing = tracing

  implicit val timeout: config.Timeout = mainConfig.http.timeout
  implicit val timeoutException: TimeoutException[Failures.Timeout] = TimeoutException(Failures.timeout)

  val endpoints: List[ZServerEndpoint[Any, Any]] = List(
    topicEndpoints.getTopicsEndpoint.zServerLogicWithTracing(EndpointUtils.timeout(_ => getTopics)),
    topicEndpoints.getTopicEndpoint.zServerLogicWithTracing(EndpointUtils.timeout(getTopic))
  )

  private def getTopics: ZIO[Any, QueryFailure, Seq[String]] = topicService.getTopics

  private def getTopic(topic: String): ZIO[Any, QueryFailure, KafkaSingleTopicPartitionInfos] = for {
    _ <- tracing.addEvent("reading topic partition infos from cache")
    _ <- tracing.setAttribute("topic", topic)
    topicPartitionInfos <- topicService.getTopicPartitionInfos(topic)
    _ <- tracing.addEvent(topicPartitionInfos match
      case Some(_) => "read topic partition infos from cache"
      case None    => "topic not found in cache"
    )
    withErrorType <- ZIO.getOrFailWith(Failures.notFound(s"topic $topic not found"))(topicPartitionInfos)
  } yield withErrorType
}

object TopicServerEndpoints {
  val live: ZLayer[TopicEndpoints & KafkaTopicInfoCache & Tracing & MainConfig, Nothing, TopicServerEndpoints] =
    ZLayer.fromFunction(TopicServerEndpoints(_, _, _, _))
}
