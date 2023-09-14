/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder, JsonFieldDecoder, JsonFieldEncoder}

object TopologyJson {
  given JsonEncoder[TopologyId] = JsonEncoder[String].contramap(_.value)
  given JsonDecoder[TopologyId] = JsonDecoder[String].map(TopologyId.apply)

  given JsonEncoder[Namespace] = JsonEncoder[String].contramap(_.value)
  given JsonDecoder[Namespace] = JsonDecoder[String].map(Namespace.apply)

  given JsonEncoder[TopicConfigValue] = JsonEncoder[String].contramap(_.value)
  given JsonDecoder[TopicConfigValue] = JsonDecoder[String].map(TopicConfigValue.apply)

  given JsonFieldEncoder[TopicConfigKey] = JsonFieldEncoder[String].contramap(_.value)
  given JsonFieldDecoder[TopicConfigKey] = JsonFieldDecoder[String].map(TopicConfigKey.apply)

  given JsonFieldEncoder[TopicId] = JsonFieldEncoder[String].contramap(_.value)
  given JsonFieldDecoder[TopicId] = JsonFieldDecoder[String].map(TopicId.apply)

  given JsonEncoder[Topic] = DeriveJsonEncoder.gen[Topic]
  given JsonDecoder[Topic] = DeriveJsonDecoder.gen[Topic]

  given JsonEncoder[Topology] = DeriveJsonEncoder.gen[Topology]
  given JsonDecoder[Topology] = DeriveJsonDecoder.gen[Topology]

  given JsonEncoder[TopologyDeployResult] = DeriveJsonEncoder.gen[TopologyDeployResult]
  given JsonDecoder[TopologyDeployResult] = DeriveJsonDecoder.gen[TopologyDeployResult]
}
