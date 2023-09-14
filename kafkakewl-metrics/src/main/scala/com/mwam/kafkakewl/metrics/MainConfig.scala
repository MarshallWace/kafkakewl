/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics

import com.mwam.kafkakewl.domain.config.{HttpConfig, KafkaClusterConfig}
import com.mwam.kafkakewl.utils.config.TypesafeConfigExtensions
import zio.*
import zio.config.magnolia.descriptor
import zio.config.typesafe.*
import zio.config.{ReadError, read}
import zio.metrics.connectors.MetricsConfig

final case class MainConfig(
  kafkaClusterConfig: KafkaClusterConfig,
  consumerOffsetsSourceConfig: ConsumerOffsetsSourceConfig,
  httpConfig: HttpConfig,
  metricsConfig: MetricsConfig
)

final case class ConsumerOffsetsSourceConfig(
  initialLoadParallelism: Int = 1,
  compactionInterval: Duration = 1.seconds,
  consumerGroup: Option[String] = None,
  consumerOffsetsTopicName: String = "__consumer_offsets",
)

object MainConfig {
  val live: ZLayer[Any, ReadError[String], MainConfig] =
    ZLayer {
      read(descriptor[MainConfig].from(
        TypesafeConfigSource.fromTypesafeConfig(
          TypesafeConfigExtensions.loadWithOverride(".kafkakewl-metrics-config-overrides.conf")
        )
      ))
    }
}

object KafkaClusterConfig {
  val live: ZLayer[MainConfig, Nothing, KafkaClusterConfig] =
    ZLayer.fromZIO {
      for {
        mainConfig <- ZIO.service[MainConfig]
      } yield mainConfig.kafkaClusterConfig
    }
}

object ConsumerOffsetsSourceConfig {
  val live: ZLayer[MainConfig, Nothing, ConsumerOffsetsSourceConfig] =
    ZLayer.fromZIO {
      for {
        mainConfig <- ZIO.service[MainConfig]
      } yield mainConfig.consumerOffsetsSourceConfig
    }
}

object HttpConfig {
  val live: ZLayer[MainConfig, Nothing, HttpConfig] =
    ZLayer.fromZIO {
      for {
        mainConfig <- ZIO.service[MainConfig]
      } yield mainConfig.httpConfig
    }
}

object MetricsConfig {
  val live: ZLayer[MainConfig, Nothing, MetricsConfig] =
    ZLayer.fromZIO {
      for {
        mainConfig <- ZIO.service[MainConfig]
      } yield mainConfig.metricsConfig
    }
}