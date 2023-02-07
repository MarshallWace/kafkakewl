/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.persistence

import com.mwam.kafkakewl.kafka.utils.KafkaConnection
import com.mwam.kafkakewl.common.changelog.ChangeLogStore
import com.mwam.kafkakewl.common.{AllDeploymentEntitiesPersistentStore, AllStateEntitiesPersistentStore, Env}
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.{AllDeploymentEntitiesStateChanges, AllStateEntitiesStateChanges, EntityStateChangeTransactionItem}
import com.typesafe.scalalogging.Logger
import io.circe.{Decoder, Encoder}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.Duration

trait PersistentStoreFactory {
  def createForStateEntities(logger: Logger, startWithWipe: Boolean): AllStateEntitiesPersistentStore
  def createForDeploymentEntities(kafkaClusterId: KafkaClusterEntityId, kafkaCluster: KafkaCluster, kafkaConnection: KafkaConnection, logger: Logger, startWithWipe: Boolean): AllDeploymentEntitiesPersistentStore
}

object PersistentStoreFactory {
  def create(
    persistentStoreType: String,
    env: Env,
    kafkaOrNone: Option[PersistentStoreConfig.Kafka],
    sqlOrNone: Option[PersistentStoreConfig.Sql],
    changeLogStore: Option[ChangeLogStore],
    timeoutDuration: Duration
  )(
    implicit
    encoderState: Encoder[EntityStateChangeTransactionItem[AllStateEntitiesStateChanges]],
    decoderState: Decoder[EntityStateChangeTransactionItem[AllStateEntitiesStateChanges]],
    encoderDeployment: Encoder[EntityStateChangeTransactionItem[AllDeploymentEntitiesStateChanges]],
    decoderDeployment: Decoder[EntityStateChangeTransactionItem[AllDeploymentEntitiesStateChanges]],
    ec: ExecutionContextExecutor
  ): (PersistentStoreConfig, PersistentStoreFactory) = {
    persistentStoreType.toLowerCase match {
      case "kafka" =>
        val config = kafkaOrNone.getOrElse(throw new RuntimeException(s"kafka persistent storage config is missing"))
        (
          config,
          new PersistentStoreFactory {
            def createForStateEntities(logger: Logger, startWithWipe: Boolean): AllStateEntitiesPersistentStore =
              new kafka.AllStateEntitiesKafkaPersistentStore(
                env,
                config.connection,
                config.stateEntitiesChangeTopicConfig,
                config.kafkaTransactionalIdForStateEntities,
                changeLogStore,
                timeoutDuration,
                logger,
                startWithWipe
              )

            def createForDeploymentEntities(kafkaClusterId: KafkaClusterEntityId, kafkaCluster: KafkaCluster, kafkaConnection: KafkaConnection, logger: Logger, startWithWipe: Boolean): AllDeploymentEntitiesPersistentStore =
              new kafka.AllDeploymentEntitiesKafkaPersistentStore(
                env,
                kafkaConnection,
                config.deploymentEntitiesChangeTopicConfig,
                kafkaClusterId,
                kafkaCluster,
                config.kafkaTransactionalIdForDeploymentEntities,
                changeLogStore,
                timeoutDuration,
                logger,
                startWithWipe
              )
          }
        )

      case "sql" =>
        val config = sqlOrNone.getOrElse(throw new RuntimeException(s"sql persistent storage config is missing"))
        (
          config,
          new PersistentStoreFactory {
            def createForStateEntities(logger: Logger, startWithWipe: Boolean): AllStateEntitiesPersistentStore =
              new sql.AllStateEntitiesSqlPersistentStore(
                env,
                config.connectionInfo,
                changeLogStore,
                timeoutDuration,
                logger,
                startWithWipe
              )

            def createForDeploymentEntities(kafkaClusterId: KafkaClusterEntityId, kafkaCluster: KafkaCluster, kafkaConnection: KafkaConnection, logger: Logger, startWithWipe: Boolean): AllDeploymentEntitiesPersistentStore =
              new sql.AllDeploymentEntitiesSqlPersistentStore(
                env,
                kafkaClusterId,
                config.connectionInfo,
                changeLogStore,
                timeoutDuration,
                logger,
                startWithWipe
              )
          }
        )

      case _ => throw new RuntimeException(s"invalid persistent-store type: $persistentStoreType")
    }
  }
}
