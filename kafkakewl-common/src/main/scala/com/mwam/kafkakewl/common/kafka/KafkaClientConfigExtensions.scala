/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.kafka

import com.mwam.kafkakewl.domain.config.KafkaClientConfig
import org.apache.kafka.clients.CommonClientConfigs

import java.util.Properties
import scala.jdk.CollectionConverters.*

extension (kafkaClientConfig: KafkaClientConfig) {
  def toProperties: java.util.Properties = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaClientConfig.brokers)
    props.putAll(kafkaClientConfig.additionalConfig.asJava)
    props
  }
}
