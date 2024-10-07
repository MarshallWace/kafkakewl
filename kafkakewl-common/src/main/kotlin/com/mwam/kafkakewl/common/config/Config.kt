/*
 * SPDX-FileCopyrightText: 2024 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.config

data class HttpConfig(
    val host: String = "0.0.0.0",
    val port: Int = 8080
)

data class KafkaPersistentStoreTopicConfig(
    val name: String = "kewl.persistent-store",
    val replicationFactor: Short? = null,
    val config: Map<String, String> = emptyMap(),
    val reCreate: Boolean = false
)

data class KafkaPersistentStoreConfig(
    val topic: KafkaPersistentStoreTopicConfig = KafkaPersistentStoreTopicConfig(),
    val transactionalId: String = "kewl.persistent-store.transactional-id"
)

data class KafkaClientConfig(
    val brokers: String,
    val config: Map<String, String> = emptyMap()
)

data class KafkaClusterConfig(
    val client: KafkaClientConfig
)
