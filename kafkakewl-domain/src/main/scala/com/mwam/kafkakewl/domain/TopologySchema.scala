/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import sttp.tapir.*

object TopologySchema {
  given [T <: StringValue]: Schema[T] = Schema(SchemaType.SString()).validate(Validator.nonEmptyString.contramap(_.value))
  given topicConfigKeyValueMapSchema: Schema[Map[TopicConfigKey, TopicConfigValue]] = Schema.schemaForMap[TopicConfigKey, TopicConfigValue](_.value)
}
