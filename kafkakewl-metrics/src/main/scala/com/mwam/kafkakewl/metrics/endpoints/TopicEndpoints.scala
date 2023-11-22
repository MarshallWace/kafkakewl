/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.endpoints

import com.mwam.kafkakewl.common.http.EndpointUtils
import com.mwam.kafkakewl.metrics.domain.{
  KafkaSingleTopicPartitionInfos,
  QueryFailure
}
import com.mwam.kafkakewl.metrics.domain.KafkaTopicPartitionInfoJson.given
import com.mwam.kafkakewl.metrics.domain.KafkaTopicPartitionInfoSchema.given
import sttp.tapir.json.zio.*
import sttp.tapir.ztapir.*
import sttp.tapir.PublicEndpoint
import zio.*

class TopicEndpoints() extends EndpointUtils with EndpointOutputs {
  private val topicEndpoint = apiEndpoint.in("topic")

  val getTopicsEndpoint: PublicEndpoint[Unit, QueryFailure, Seq[String], Any] =
    topicEndpoint.get
      .errorOut(queryFailureOutput)
      .out(jsonBody[Seq[String]])

  val getTopicEndpoint: PublicEndpoint[
    String,
    QueryFailure,
    KafkaSingleTopicPartitionInfos,
    Any
  ] = topicEndpoint
    .in(path[String]("topic_name"))
    .get
    .errorOut(queryFailureOutput)
    .out(jsonBody[KafkaSingleTopicPartitionInfos])
}

object TopicEndpoints {
  val live: ZLayer[Any, Nothing, TopicEndpoints] =
    ZLayer.succeed(TopicEndpoints())
}
