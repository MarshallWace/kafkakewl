/*
 * SPDX-FileCopyrightText: 2025 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.config

import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.addFileSource
import com.sksamuel.hoplite.addResourceSource
import com.sksamuel.hoplite.sources.EnvironmentVariablesPropertySource

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
    val name: String,
    val client: KafkaClientConfig
)


fun ConfigLoaderBuilder.addEnvironmentSource(prefix: String? = null): ConfigLoaderBuilder {
    return addPropertySource(
        EnvironmentVariablesPropertySource(
            useUnderscoresAsSeparator = true,
            allowUppercaseNames = true,
            { System.getenv().filter { prefix == null || it.key.startsWith(prefix) != false } },
            prefix = prefix
        )
    )
}

inline fun <reified A : Any> loadConfig(overrideFileName: String, applicationConfigResource: String = "/application.yaml"): A {
    return ConfigLoaderBuilder.default()
        .addEnvironmentSource(prefix = "KAFKAKEWL__")
        .addFileSource(overrideFileName)
        .addResourceSource(applicationConfigResource)
        .build()
        .loadConfigOrThrow<A>()
}
