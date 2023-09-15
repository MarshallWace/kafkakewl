/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy

import com.mwam.kafkakewl.domain.config.{HttpConfig, KafkaClusterConfig}
import com.mwam.kafkakewl.utils.config.TypesafeConfigExtensions
import zio.config.magnolia.descriptor
import zio.config.typesafe.*
import zio.config.{ReadError, read}
import zio.metrics.connectors.MetricsConfig
import zio.{ZIO, ZLayer}

final case class MainConfig(
  kafkaClusterConfig: KafkaClusterConfig,
  httpConfig: HttpConfig,
  metricsConfig: MetricsConfig
)

object MainConfig {
  val live: ZLayer[Any, ReadError[String], MainConfig] =
    ZLayer {
      read(descriptor[MainConfig].from(
        TypesafeConfigSource.fromTypesafeConfig(
          TypesafeConfigExtensions.loadWithOverride(".kafkakewl-deploy-config-overrides.conf")
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