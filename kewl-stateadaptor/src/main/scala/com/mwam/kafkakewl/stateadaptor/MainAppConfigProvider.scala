/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.stateadaptor

import com.mwam.kafkakewl.kafka.utils._
import com.mwam.kafkakewl.common.ConfigProvider
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.stateadaptor.destinations.LatestDeployedTopologyStateAdaptor
import com.mwam.kafkakewl.utils._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContextExecutor

trait MainAppConfigProvider extends ConfigProvider {
  this: LazyLogging =>

  implicit val executionContext: ExecutionContextExecutor

  val config: Config = loadConfigWithDefaultsAndOverrides(prefix = ".kafkakewl-stateadaptor")
  val kafkaKewlStateAdaptorConfig: Config = config.getConfig("kafkakewl-stateadaptor")

  config.getStringOrNoneIfEmpty("wait-for-file-at-startup.name").foreach { WaitForFile.filePath(_) }

  val kafkaChangeLogStoreConnection: KafkaConnection = kafkaKewlStateAdaptorConfig.kafkaConnection("changelog-store.kafka-cluster")
    .getOrElse(throw new RuntimeException("kafkakewl-stateadaptor.changelog-store.kafka-cluster must be specified"))

  val destinationLatestDeployedTopologyStateJaasConfig: Option[String] = kafkaKewlStateAdaptorConfig.getStringOrNoneIfEmpty("destinations.latest-deployedtopology-state.jaas-config")
  val destinationLatestDeployedTopologyStateClusterConfigs: Seq[LatestDeployedTopologyStateAdaptor.Config] = kafkaKewlStateAdaptorConfig.getConfigList("destinations.latest-deployedtopology-state.clusters").asScala
    .flatMap(c => for {
      consumerGroupId <- c.getStringOrNone("consumer-group-id")
      id <- c.getStringOrNone("id")
      topic <- c.getStringOrNone("topic")
      info <- c.kafkaConnectionInfo("")
    } yield LatestDeployedTopologyStateAdaptor.Config(consumerGroupId, KafkaClusterEntityId(id), KafkaConnection(info, destinationLatestDeployedTopologyStateJaasConfig), topic))

  val destinationLatestGlobalStateKafkaCluster: KafkaConnection = kafkaKewlStateAdaptorConfig.kafkaConnectionOrNone("destinations.latest-global-state.kafka-cluster")
    .getOrElse(throw new RuntimeException("destinations.latest-global-state.kafka-cluster must be specified"))
  val destinationLatestGlobalStateTopic: String = kafkaKewlStateAdaptorConfig.getString("destinations.latest-global-state.topic")
  val destinationLatestGlobalStateConsumerGroup: String = kafkaKewlStateAdaptorConfig.getString("destinations.latest-global-state.consumer-group-id")
}
