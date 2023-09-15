/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.persistence

final case class KafkaPersistentStoreTopicConfig(
  name: String = "kewl.persistent-store",
  replicationFactor: Option[Short] = None,
  config: Map[String, String] = Map.empty,
  reCreate: Boolean = false
)
final case class KafkaPersistentStoreConfig(
  topic: KafkaPersistentStoreTopicConfig = KafkaPersistentStoreTopicConfig()
)
