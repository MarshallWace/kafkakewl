/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.kafka

import com.mwam.kafkakewl.domain.config.KafkaClientConfig
import zio.*
import zio.kafka.consumer.{Consumer, ConsumerSettings}

object KafkaConsumer {
  val live: ZLayer[KafkaClientConfig, Throwable, Consumer] =
    ZLayer.scoped(
      for {
        kafkaClientConfig <- ZIO.service[KafkaClientConfig]
        consumer <- Consumer.make(
          ConsumerSettings(
            kafkaClientConfig.brokersList,
            properties = kafkaClientConfig.additionalConfig
          )
        )
      } yield consumer
    )
}
