/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.persistence.kafka

import com.mwam.kafkakewl.kafka.utils.KafkaExtensions._
import com.mwam.kafkakewl.kafka.utils.{KafkaConfigProperties, KafkaConnection, KafkaConsumerConfig, KafkaProducerConfig}
import com.mwam.kafkakewl.common.AllStateEntities.InMemoryVersionedStateStores
import com.mwam.kafkakewl.common.AllStateEntities.InMemoryVersionedStateStores._
import com.mwam.kafkakewl.common.AllStateEntities.WritableStateStores._
import com.mwam.kafkakewl.common._
import com.mwam.kafkakewl.common.changelog.ChangeLogStore
import com.mwam.kafkakewl.domain.{AllStateEntitiesStateChanges, EntityStateChangeTransactionItem}
import com.mwam.kafkakewl.utils.ApplicationMetrics
import com.typesafe.scalalogging.Logger
import io.circe.{Decoder, Encoder}
import org.apache.kafka.clients.producer.KafkaProducer

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor}

class AllStateEntitiesKafkaPersistentStore(
  env: Env,
  connection: KafkaConnection,
  systemTopicConfig: KafkaKewlSystemTopicConfig,
  kafkaProducerTransactionalId: String,
  changeLogStore: Option[ChangeLogStore],
  timeoutDuration: Duration,
  logger: Logger,
  startWithWipe: Boolean
)(
  implicit
  encoder: Encoder[EntityStateChangeTransactionItem[AllStateEntitiesStateChanges]],
  decoder: Decoder[EntityStateChangeTransactionItem[AllStateEntitiesStateChanges]],
  ec: ExecutionContextExecutor
) extends AllStateEntitiesPersistentStore {

  private[this] val stateChangesTopic = KafkaKewlTopicUtils.changes(env, AllStateEntities.name)

  KafkaKewlTopicUtils.createIfNeeded(env, connection, systemTopicConfig, Seq(stateChangesTopic), wipeAndRecreate = startWithWipe)
  if (startWithWipe) {
    // Wiping the change log too. In theory it's possible that after it's wiped some pending kafka-cluster message processing
    // produces new messages into here, before the kafka-cluster processor is also shut down, but it's unlikely and the admin
    // can re-do the wipe in that case.
    changeLogStore.foreach(_.wipe())
  }

  private[this] val producerConfig = KafkaProducerConfig(connection, idempotent = Some(true), transactionalId = Some(kafkaProducerTransactionalId))
  private[this] val producer: KafkaProducer[String, String] = {
    val producer = new KafkaProducer[String, String](KafkaConfigProperties.forProducer(producerConfig))
    producer.initTransactions()
    producer
  }

  def loadAllVersions(): InMemoryVersionedStateStores = {
    val stateStore = AllStateEntities.InMemoryVersionedStateStores()
    KafkaEntityStateChangeTransactions.consume[AllStateEntitiesStateChanges](
      env,
      KafkaConsumerConfig(connection, groupId = None),
      stateChangesTopic,
      stateStore.toWritable.applyEntityStateChange
    )
    stateStore
  }

  def saveTransactions(transactionItems: IndexedSeq[EntityStateChangeTransactionItem[AllStateEntitiesStateChanges]]): Unit = {
    changeLogStore.foreach(_.saveStateChanges(transactionItems))

    producer.inTransaction(
      p => Await.result(KafkaEntityStateChangeTransactions.produce(env, p, stateChangesTopic, transactionItems), timeoutDuration),
      beforeRetry = e => {
        logger.error(s"KafkaError while producing state entity changes", e)
        ApplicationMetrics.errorCounter.inc()
      }
    )
  }

  def wipe(): Unit = KafkaKewlTopicUtils.createIfNeeded(env, connection, systemTopicConfig, Seq(stateChangesTopic), wipeAndRecreate = true)
  def close(): Unit = producer.close()
}
