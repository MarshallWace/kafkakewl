/*
 * SPDX-FileCopyrightText: 2024 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.plugins

import com.mwam.kafkakewl.common.config.KafkaClientConfig
import com.mwam.kafkakewl.common.config.KafkaPersistentStoreConfig
import com.mwam.kafkakewl.common.persistence.KafkaPersistentStore
import com.mwam.kafkakewl.common.persistence.PersistentStore
import com.mwam.kafkakewl.common.plugins.koinModuleForMetrics
import com.mwam.kafkakewl.metrics.Config
import com.mwam.kafkakewl.metrics.services.*
import io.ktor.server.application.*
import org.koin.dsl.module
import org.koin.ktor.plugin.Koin
import org.koin.logger.slf4jLogger

fun Application.configureFrameworks(config: Config) {
    install(Koin) {
        slf4jLogger()
        modules(
            koinModuleForMetrics(),
            module {
                single<Config> { config }
                single<KafkaClientConfig> { (get<Config>().kafkaCluster.client) }
                single<KafkaPersistentStoreConfig> { config.kafkaPersistentStore }
                single<PersistentStore> { KafkaPersistentStore(get(), get()) }
                single<KafkaTopicInfoSource> { KafkaTopicInfoSourceImpl(get(), config.topicInfoSource) }
                single<KafkaTopicInfoCache>(createdAtStart=true) { KafkaTopicInfoCacheImpl(get()) }
            }
        )
    }
}
