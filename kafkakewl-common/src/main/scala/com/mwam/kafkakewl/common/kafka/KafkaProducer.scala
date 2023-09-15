/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.kafka

import com.mwam.kafkakewl.domain.config.KafkaClientConfig
import zio.*
import zio.kafka.producer.*

object KafkaProducer {
  val live: ZLayer[KafkaClientConfig, Throwable, Producer] =
    ZLayer.scoped {
      for {
        kafkaClientConfig <- ZIO.service[KafkaClientConfig]
        consumer <- Producer.make(
          ProducerSettings(kafkaClientConfig.brokersList, properties = kafkaClientConfig.additionalConfig)
        )
      } yield consumer
    }
}
