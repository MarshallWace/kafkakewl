/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.endpoints

import com.mwam.kafkakewl.common.http.{EndpointUtils, ErrorResponse}
import com.mwam.kafkakewl.metrics.domain.KafkaSingleTopicPartitionInfos
import com.mwam.kafkakewl.metrics.domain.KafkaTopicPartitionInfoJson.given
import com.mwam.kafkakewl.metrics.domain.KafkaTopicPartitionInfoSchema.given
import sttp.tapir.json.zio.*
import sttp.tapir.ztapir.*
import sttp.tapir.{EndpointOutput, PublicEndpoint}
import zio.*

class TopicEndpoints() extends EndpointUtils {
  private val topicEndpoint = apiEndpoint.in("topic")

  val getTopicsEndpoint: PublicEndpoint[Unit, Unit, Seq[String], Any] = topicEndpoint
    .get
    .out(jsonBody[Seq[String]])

  val getTopicEndpoint: PublicEndpoint[String, ErrorResponse, KafkaSingleTopicPartitionInfos, Any] = topicEndpoint
    .in(path[String]("topic_name"))
    .get
    .errorOut(EndpointOutput.derived[ErrorResponse])
    .out(jsonBody[KafkaSingleTopicPartitionInfos])
}

object TopicEndpoints {
  val live: ZLayer[Any, Nothing, TopicEndpoints] = ZLayer.succeed(TopicEndpoints())
}
