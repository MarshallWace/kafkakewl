/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.migrate

import com.mwam.kafkakewl.common.ConfigProvider
import com.mwam.kafkakewl.common.http.HttpConfigProvider
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.kafka.utils.{CommonConfigExtensions, KafkaConnection}
import com.mwam.kafkakewl.utils.{ConfigExtensions, ExecutorContextFactory, WaitForFile}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

trait HttpServerAppConfigProvider extends ExecutorContextFactory with HttpConfigProvider with ConfigProvider {
  this: LazyLogging =>

  val config: Config = loadConfigWithDefaultsAndOverrides(prefix = ".kafkakewl-migrate")
  private val kafkaKewlMigrateConfig: Config = config.getConfig("kafkakewl-migrate")
  val configForHttp: Config = kafkaKewlMigrateConfig

  config.getStringOrNoneIfEmpty("wait-for-file-at-startup.name").foreach { WaitForFile.filePath(_) }

  val kafkaChangeLogStoreConnection: KafkaConnection = kafkaKewlMigrateConfig.kafkaConnection("changelog-store.kafka-cluster")
    .getOrElse(throw new RuntimeException("changelog-store.kafka-cluster must be specified"))

  val vnextInstances = kafkaKewlMigrateConfig.kafkaConfig("vnext.instances").map { case (k, v) => (KafkaClusterEntityId(k), v)}
  val vnextConnectTimeoutMillis = kafkaKewlMigrateConfig.getIntOrNoneIfEmpty("vnext.connect-timeout-millis").getOrElse(30000)
  val vnextReadTimeoutMillis = kafkaKewlMigrateConfig.getIntOrNoneIfEmpty("vnext.read-timeout-millis").getOrElse(120000)

  val destinationPath: Option[String] = kafkaKewlMigrateConfig.getStringOrNone("destination.path")
}
