/*
 * SPDX-FileCopyrightText: 2024 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics

import com.mwam.kafkakewl.common.config.HttpConfig
import com.mwam.kafkakewl.common.config.KafkaClusterConfig
import com.mwam.kafkakewl.common.config.KafkaPersistentStoreConfig
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

data class TopicInfoSourceConfig(
    val pollInterval: Duration = 2.seconds,
    val excludeTopicRegexes: List<String> = emptyList(),
    val timeout: Duration = 60.seconds
)

data class Config(
    val http: HttpConfig = HttpConfig(),
    val kafkaCluster: KafkaClusterConfig,
    val kafkaPersistentStore: KafkaPersistentStoreConfig = KafkaPersistentStoreConfig(),
    val topicInfoSource: TopicInfoSourceConfig = TopicInfoSourceConfig()
)
