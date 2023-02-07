/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.kafka.utils

import java.util.Properties
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SaslConfigs

object KafkaConfigProperties {
  private def addConfigToProperties(config: Map[String, String], props: Properties): Properties = {
    // We can't use putAll here because of this bug: https://github.com/scala/bug/issues/10418
    for ((k, v) <- config) {
      props.put(k, v)
    }
    props
  }

  def forProducer(
    config: KafkaProducerConfig,
    keySerializer: String = "org.apache.kafka.common.serialization.StringSerializer",
    valueSerializer: String = "org.apache.kafka.common.serialization.StringSerializer"
  ): Properties = {

    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.connection.info.brokers)
    config.connection.info.securityProtocol.foreach { securityProtocol => props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol) }
    config.connection.jaasConfig.foreach { jaasConfig =>
      props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig)
    }
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer)
    config.transactionalId.foreach { transactionalId => props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId) }
    config.idempotent.foreach { idempotent => props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotent : java.lang.Boolean) }
    addConfigToProperties(config.connection.info.config, props)
  }

  def forAdmin(config: KafkaAdminConfig): Properties = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.connection.info.brokers)
    config.connection.info.securityProtocol.foreach { securityProtocol => props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol) }
    config.connection.jaasConfig.foreach { jaasConfig =>
      props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig)
    }
    config.kafkaRequestTimeOutMillis.foreach { kafkaRequestTimeOutMillis =>
      props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, kafkaRequestTimeOutMillis : java.lang.Integer)
    }
    addConfigToProperties(config.connection.info.config, props)
  }

  def forConsumer(config: KafkaConsumerConfig, keyDeserializer: String, valueDeserializer: String): Properties = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.connection.info.brokers)
    config.connection.info.securityProtocol.foreach { securityProtocol => props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol) }
    config.connection.jaasConfig.foreach { jaasConfig =>
      props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig)
    }
    config.groupId.foreach { groupId => props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId) }
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.enableAutoCommit : java.lang.Boolean)
    config.autoOffsetReset.foreach { autoOffsetReset => props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset) }
    config.isolationLevel.foreach { isolationLevel => props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel) }
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer)
    config.kafkaRequestTimeOutMillis.foreach { kafkaRequestTimeOutMillis =>
      props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, kafkaRequestTimeOutMillis : java.lang.Integer)
    }
    addConfigToProperties(config.connection.info.config, props)
  }

  def forBytesConsumer(config: KafkaConsumerConfig): Properties =
    forConsumer(
      config,
      keyDeserializer = "org.apache.kafka.common.serialization.BytesDeserializer",
      valueDeserializer = "org.apache.kafka.common.serialization.BytesDeserializer"
    )

  def forStringConsumer(config: KafkaConsumerConfig): Properties =
    forConsumer(
      config,
      keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer",
      valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer"
    )
}
