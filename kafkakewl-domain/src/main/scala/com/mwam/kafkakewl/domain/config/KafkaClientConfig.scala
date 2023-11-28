/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain.config

import zio.{ZIO, ZLayer}

final case class KafkaClientConfig(
    brokers: String,
    additionalConfig: Map[String, String]
) {
  lazy val brokersList: List[String] = brokers.split(',').map(_.trim).filter(_.nonEmpty).toList
}

object KafkaClientConfig {
  val live: ZLayer[KafkaClusterConfig, Nothing, KafkaClientConfig] =
    ZLayer.fromZIO {
      for {
        kafkaClusterConfig <- ZIO.service[KafkaClusterConfig]
      } yield kafkaClusterConfig.client
    }
}
