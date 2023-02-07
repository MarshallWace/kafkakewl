/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api

import com.mwam.kafkakewl.common.http.HttpConfigProvider
import com.mwam.kafkakewl.common.persistence.PersistentStoreConfig
import com.mwam.kafkakewl.common.{ConfigProvider, Env, KafkaKewlSystemTopicConfig}
import com.mwam.kafkakewl.domain.FlexibleName
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.kafka.utils._
import com.mwam.kafkakewl.utils._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContextExecutor

trait HttpServerAppConfigProvider extends HttpConfigProvider with ConfigProvider {
  this: LazyLogging =>

  implicit val executionContext: ExecutionContextExecutor

  val config: Config = loadConfigWithDefaultsAndOverrides(prefix = ".kafkakewl-api")
  val kafkaKewlApiConfig: Config = config.getConfig("kafkakewl-api")
  val configForHttp: Config = kafkaKewlApiConfig

  config.getStringOrNoneIfEmpty("wait-for-file-at-startup.name").foreach { WaitForFile.filePath(_) }

  val env: Env = kafkaKewlApiConfig.getStringOrNoneIfEmpty(s"env")
    .flatMap(Env(_))
    .getOrElse(throw new RuntimeException(s"env config value must be set to a valid environment: dev or prod"))

  val permissionPluginName: Option[String] = kafkaKewlApiConfig.getStringOrNoneIfEmpty(s"permission-plugin-name")

  val topicDefaults: TopicDefaults = TopicDefaults(
    kafkaKewlApiConfig.getStringOrNoneIfEmpty("topic-defaults.otherConsumerNamespaces").map(FlexibleName.decodeFromJson(_).right.get).getOrElse(Seq.empty),
    kafkaKewlApiConfig.getStringOrNoneIfEmpty("topic-defaults.otherProducerNamespaces").map(FlexibleName.decodeFromJson(_).right.get).getOrElse(Seq.empty)
  )

  val metricsServiceUri: Option[String] = kafkaKewlApiConfig.getStringOrNoneIfEmpty(s"metrics-service.uri")

  val superUsers: Seq[String] = kafkaKewlApiConfig.getStringOrNoneIfEmpty(s"super-users").map(_.split(",").toSeq).getOrElse(Seq.empty)

  def systemTopicConfig(path: String): Option[KafkaKewlSystemTopicConfig] = for {
    replicationFactor <- kafkaKewlApiConfig.getShortOrNoneIfEmpty(s"$path.system-topic-config.replicationFactor")
  } yield {
    val kafkaConfig = kafkaKewlApiConfig.kafkaConfig(s"$path.system-topic-config.kafka-topic-config")
    KafkaKewlSystemTopicConfig(replicationFactor, kafkaConfig)
  }

  val kafkaPersistentStoreConfigOrNone: Option[PersistentStoreConfig.Kafka] = for {
    kafkaConnection <- kafkaKewlApiConfig.kafkaConnectionOrNone(s"state-command-processor.kafka-cluster")
    kafkaTransactionalIdForStateEntities <- kafkaKewlApiConfig.getStringOrNoneIfEmpty(s"state-command-processor.kafka-transactional-id")
    kafkaTransactionalIdForDeploymentEntities <- kafkaKewlApiConfig.getStringOrNoneIfEmpty(s"kafkacluster-command-processor.kafka-transactional-id")
  } yield PersistentStoreConfig.Kafka(
    kafkaConnection,
    systemTopicConfig(s"state-command-processor").getOrElse(KafkaKewlSystemTopicConfig()),
    systemTopicConfig(s"kafkacluster-command-processor").getOrElse(KafkaKewlSystemTopicConfig()),
    kafkaTransactionalIdForStateEntities,
    kafkaTransactionalIdForDeploymentEntities
  )

  val sqlDbConnectionInfoOrNone: Option[SqlDbConnectionInfo] = kafkaKewlApiConfig.sqlDbConnectionInfoOrNone(s"sql-persistent-store")
  val sqlPersistentStoreConfigOrNone: Option[PersistentStoreConfig.Sql] = sqlDbConnectionInfoOrNone
    .map(s => s.resolve(kafkaKewlApiConfig.getString(s"sql-persistent-store.suffix")))
    .map(PersistentStoreConfig.Sql)

  val persistentStore: String = kafkaKewlApiConfig.getString(s"persistent-store").toLowerCase

  val kafkaClusterCommandProcessorJaasConfig: Option[String] = kafkaKewlApiConfig.getStringOrNoneIfEmpty(s"kafkacluster-command-processor.kafka-cluster.jaas-config")

  val kafkaChangeLogStoreConnectionOrNone: Option[KafkaConnection] = kafkaKewlApiConfig.kafkaConnectionOrNone(s"changelog-store.kafka-cluster")
  val kafkaChangeLogStoreTopicConfig: KafkaKewlSystemTopicConfig = systemTopicConfig(s"changelog-store").getOrElse(KafkaKewlSystemTopicConfig())

  val failFastIfStateStoreInvalid: Boolean = kafkaKewlApiConfig.getBooleanOrNone(s"state-command-processor.fail-fast-if-stateStore-invalid").getOrElse(true)
  val failFastIfDeploymentStateStoreInvalid: Boolean = kafkaKewlApiConfig.getBooleanOrNone(s"kafkacluster-command-processor.fail-fast-if-deployment-stateStore-invalid").getOrElse(true)
}
