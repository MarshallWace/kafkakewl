/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.kafka

import org.apache.kafka.clients.admin.ConfigEntry
import zio.*
import zio.kafka.admin.*

object AdminClientExtensions {
  import AdminClient.*

  extension (adminClient: AdminClient) {
    def getTopicConfigEntries(topic: String): Task[Map[String, ConfigEntry]] = {
      val topicConfigResource = ConfigResource(ConfigResourceType.Topic, topic)
      for {
        configs <- adminClient.describeConfigs(Seq(topicConfigResource))
        config <- ZIO.getOrFailWith(new RuntimeException(s"Could not describe config for topic $topic"))(configs.get(topicConfigResource))
      } yield config.entries
    }

    def getDynamicTopicConfig(topic: String): Task[Map[String, String]] = for {
      configEntries <- adminClient.getTopicConfigEntries(topic)
    } yield configEntries
      .filter((_, v) => v.source == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) // Ignoring inherited / default configs
      .map((k, v) => (k, v.value))
  }
}
