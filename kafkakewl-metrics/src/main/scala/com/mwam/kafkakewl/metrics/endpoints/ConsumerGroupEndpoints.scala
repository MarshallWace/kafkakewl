/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.endpoints

import com.mwam.kafkakewl.common.http.EndpointUtils
import com.mwam.kafkakewl.metrics.domain.{KafkaConsumerGroupInfo, QueryFailure}
import com.mwam.kafkakewl.metrics.domain.KafkaConsumerGroupInfoJson.given
import com.mwam.kafkakewl.metrics.domain.KafkaConsumerGroupInfoSchema.given
import sttp.tapir.json.zio.*
import sttp.tapir.ztapir.*
import sttp.tapir.PublicEndpoint
import zio.*

class ConsumerGroupEndpoints() extends EndpointUtils with EndpointOutputs {
  private val groupEndpoint: PublicEndpoint[Unit, Unit, Unit, Any] = apiEndpoint.in("group")

  val getGroupsEndpoint: PublicEndpoint[Unit, QueryFailure, Seq[String], Any] = groupEndpoint
    .get
    .errorOut(queryFailureOutput)
    .out(jsonBody[Seq[String]])

  val getGroupEndpoint: PublicEndpoint[String, QueryFailure, KafkaConsumerGroupInfo, Any] = groupEndpoint
    .in(path[String]("group_name"))
    .get
    .errorOut(queryFailureOutput)
    .out(jsonBody[KafkaConsumerGroupInfo])
}

object ConsumerGroupEndpoints {
  val live: ZLayer[Any, Nothing, ConsumerGroupEndpoints] = ZLayer.succeed(ConsumerGroupEndpoints())
}
