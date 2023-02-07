/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.persistence.kafka

import com.mwam.kafkakewl.kafka.utils.KafkaExtensions._
import com.mwam.kafkakewl.kafka.utils.{KafkaConfigProperties, KafkaConnection, KafkaConsumerConfig, KafkaProducerConfig}
import com.mwam.kafkakewl.common.AllDeploymentEntities.InMemoryStateStores
import com.mwam.kafkakewl.common.AllDeploymentEntities.InMemoryStateStores._
import com.mwam.kafkakewl.common.AllDeploymentEntities.WritableStateStores._
import com.mwam.kafkakewl.common._
import com.mwam.kafkakewl.common.changelog.ChangeLogStore
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.{AllDeploymentEntitiesStateChanges, EntityStateChangeTransactionItem}
import com.mwam.kafkakewl.utils.ApplicationMetrics
import com.typesafe.scalalogging.Logger
import io.circe.{Decoder, Encoder}
import org.apache.kafka.clients.producer.KafkaProducer

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor}

class AllDeploymentEntitiesKafkaPersistentStore(
  env: Env,
  connection: KafkaConnection,
  systemTopicConfig: KafkaKewlSystemTopicConfig,
  kafkaClusterId: KafkaClusterEntityId,
  kafkaCluster: KafkaCluster,
  kafkaProducerTransactionalId: String,
  changeLogStore: Option[ChangeLogStore],
  timeoutDuration: Duration,
  logger: Logger,
  startWithWipe: Boolean
)(
  implicit
  encoder: Encoder[EntityStateChangeTransactionItem[AllDeploymentEntitiesStateChanges]],
  decoder: Decoder[EntityStateChangeTransactionItem[AllDeploymentEntitiesStateChanges]],
  ec: ExecutionContextExecutor
) extends AllDeploymentEntitiesPersistentStore {

  private[this] val stateChangesTopic = KafkaKewlTopicUtils.changes(env, AllDeploymentEntities.name)

  KafkaKewlTopicUtils.createIfNeeded(env, connection, systemTopicConfig, Seq(stateChangesTopic), wipeAndRecreate = startWithWipe, resolveTopicConfig = kafkaCluster.resolveSystemTopicConfig)

  private[this] val producerConfig = KafkaProducerConfig(connection, idempotent = Some(true), transactionalId = Some(kafkaProducerTransactionalId))
  private[this] val producer: KafkaProducer[String, String] = {
    val producer = new KafkaProducer[String, String](KafkaConfigProperties.forProducer(producerConfig))
    producer.initTransactions()
    producer
  }

  def loadLatestVersions(): InMemoryStateStores = {
    val stateStore = AllDeploymentEntities.InMemoryStateStores()
    KafkaEntityStateChangeTransactions.consume[AllDeploymentEntitiesStateChanges](
      env,
      KafkaConsumerConfig(connection, groupId = None),
      stateChangesTopic,
      stateStore.toWritable.applyEntityStateChange
    )
    stateStore
  }

  def saveTransactions(transactionItems: IndexedSeq[EntityStateChangeTransactionItem[AllDeploymentEntitiesStateChanges]]): Unit = {
    changeLogStore.foreach(_.saveDeploymentStateChanges(kafkaClusterId, transactionItems))

    producer.inTransaction(
      p => Await.result(KafkaEntityStateChangeTransactions.produce(env, p, stateChangesTopic, transactionItems), timeoutDuration),
      beforeRetry = e => {
        logger.error(s"KafkaError while producing deployment entity changes", e)
        ApplicationMetrics.errorCounter.inc()
      }
    )
  }

  def wipe(): Unit = KafkaKewlTopicUtils.createIfNeeded(env, connection, systemTopicConfig, Seq(stateChangesTopic), wipeAndRecreate = true)
  def close(): Unit = producer.close()
}
