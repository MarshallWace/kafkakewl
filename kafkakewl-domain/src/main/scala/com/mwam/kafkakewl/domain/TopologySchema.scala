/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import sttp.tapir.*
import sttp.tapir.generic.auto.*

object TopologySchema {
  given Schema[TopologyId] = Schema(SchemaType.SString()).validate(Validator.nonEmptyString.contramap(_.value))
  given Schema[Namespace] = Schema(SchemaType.SString()).validate(Validator.nonEmptyString.contramap(_.value))
  given Schema[TopicId] = Schema(SchemaType.SString()).validate(Validator.nonEmptyString.contramap(_.value))
  given topicIdTopicMapSchema: Schema[Map[TopicId, Topic]] = Schema.schemaForMap[TopicId, Topic](_.value)
  given Schema[TopicConfigKey] = Schema(SchemaType.SString()).validate(Validator.nonEmptyString.contramap(_.value))
  given Schema[TopicConfigValue] = Schema(SchemaType.SString()).validate(Validator.nonEmptyString.contramap(_.value))
  given topicConfigKeyValueMapSchema: Schema[Map[TopicConfigKey, TopicConfigValue]] = Schema.schemaForMap[TopicConfigKey, TopicConfigValue](_.value)
}