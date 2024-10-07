/*
 * SPDX-FileCopyrightText: 2024 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy

import com.mwam.kafkakewl.common.config.HttpConfig
import com.mwam.kafkakewl.common.config.KafkaClusterConfig
import com.mwam.kafkakewl.common.config.KafkaPersistentStoreConfig

data class Config(
    val http: HttpConfig = HttpConfig(),
    val kafkaCluster: KafkaClusterConfig,
    val kafkaPersistentStore: KafkaPersistentStoreConfig = KafkaPersistentStoreConfig()
)
