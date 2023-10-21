/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder, JsonFieldDecoder, JsonFieldEncoder}

object TopologyJson {
  given [T <: StringValue]: JsonEncoder[T] = JsonEncoder[String].contramap(_.value)
  given [T <: StringValue]: JsonFieldEncoder[T] = JsonFieldEncoder[String].contramap(_.value)

  // JsonDecoders for StringValues - TODO we can remove this repetitive code using Mirrors
  given JsonDecoder[TopicConfigValue] = JsonDecoder[String].map(TopicConfigValue.apply)
  given JsonFieldDecoder[TopicConfigKey] = JsonFieldDecoder[String].map(TopicConfigKey.apply)
  given JsonDecoder[TopicId] = JsonDecoder[String].map(TopicId.apply)
  given JsonDecoder[ApplicationLocalId] = JsonDecoder[String].map(ApplicationLocalId.apply)
  given JsonDecoder[UserId] = JsonDecoder[String].map(UserId.apply)
  given JsonDecoder[TopicAliasLocalId] = JsonDecoder[String].map(TopicAliasLocalId.apply)
  given JsonDecoder[ApplicationAliasLocalId] = JsonDecoder[String].map(ApplicationAliasLocalId.apply)
  given JsonDecoder[ApplicationFlexId] = JsonDecoder[String].map(ApplicationFlexId.apply)
  given JsonDecoder[TopicFlexId] = JsonDecoder[String].map(TopicFlexId.apply)
  given JsonDecoder[TopologyId] = JsonDecoder[String].map(TopologyId.apply)
  given JsonDecoder[Namespace] = JsonDecoder[String].map(Namespace.apply)
  given JsonDecoder[Developer] = JsonDecoder[String].map(Developer.apply)
  given JsonFieldDecoder[TopologyId] = JsonFieldDecoder[String].map(TopologyId.apply)

  // generating JsonEncoders/JsonDecoders for normal domain types
  given JsonEncoder[Topic] = DeriveJsonEncoder.gen[Topic]
  given JsonDecoder[Topic] = DeriveJsonDecoder.gen[Topic]
  given JsonEncoder[Application] = DeriveJsonEncoder.gen[Application]
  given JsonDecoder[Application] = DeriveJsonDecoder.gen[Application]
  given JsonEncoder[TopicAlias] = DeriveJsonEncoder.gen[TopicAlias]
  given JsonDecoder[TopicAlias] = DeriveJsonDecoder.gen[TopicAlias]
  given JsonEncoder[ApplicationAlias] = DeriveJsonEncoder.gen[ApplicationAlias]
  given JsonDecoder[ApplicationAlias] = DeriveJsonDecoder.gen[ApplicationAlias]
  given JsonEncoder[Aliases] = DeriveJsonEncoder.gen[Aliases]
  given JsonDecoder[Aliases] = DeriveJsonDecoder.gen[Aliases]
  given JsonEncoder[ProducedTopic] = DeriveJsonEncoder.gen[ProducedTopic]
  given JsonDecoder[ProducedTopic] = DeriveJsonDecoder.gen[ProducedTopic]
  given JsonEncoder[ConsumedTopic] = DeriveJsonEncoder.gen[ConsumedTopic]
  given JsonDecoder[ConsumedTopic] = DeriveJsonDecoder.gen[ConsumedTopic]
  given JsonEncoder[Relationship] = DeriveJsonEncoder.gen[Relationship]
  given JsonDecoder[Relationship] = DeriveJsonDecoder.gen[Relationship]
  given JsonEncoder[Topology] = DeriveJsonEncoder.gen[Topology]
  given JsonDecoder[Topology] = DeriveJsonDecoder.gen[Topology]
}
