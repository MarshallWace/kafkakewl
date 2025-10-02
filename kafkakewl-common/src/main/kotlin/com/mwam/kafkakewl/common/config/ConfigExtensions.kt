/*
 * SPDX-FileCopyrightText: 2025 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.config

import org.apache.kafka.clients.CommonClientConfigs

fun KafkaClientConfig.toKafkaClientConfig(): Map<String, String> =
    config + mapOf(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to brokers)
