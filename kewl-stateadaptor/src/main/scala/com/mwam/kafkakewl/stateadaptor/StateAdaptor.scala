/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.stateadaptor

import java.util.concurrent.atomic.AtomicBoolean

import scala.reflect.runtime.universe._
import scala.concurrent.{Await, ExecutionContextExecutor, Future, blocking}
import com.mwam.kafkakewl.kafka.utils.{KafkaConnection, KafkaConsumerConfig}
import com.mwam.kafkakewl.common.ApplyStateChange
import com.typesafe.scalalogging.LazyLogging
import com.mwam.kafkakewl.utils._
import com.mwam.kafkakewl.kafka.utils.KafkaExtensions._
import com.mwam.kafkakewl.domain.Entity._
import com.mwam.kafkakewl.common.changelog.{KafkaChangeLogStore, ReceivedChangeLogMessage}
import com.mwam.kafkakewl.domain.{Entity, EntityState}
import com.mwam.kafkakewl.domain.JsonEncodeDecode._
import io.circe.Encoder
import org.apache.kafka.clients.producer.KafkaProducer

import scala.concurrent.duration.Duration

abstract class StateAdaptor(implicit ec: ExecutionContextExecutor) extends LazyLogging {
  protected val stopConsuming = new AtomicBoolean(false)

  logger.info("starting consuming")
  private val consumeFuture = Future { blocking { consume() } }
  consumeFuture.crashIfFailed()
  logger.info("consumer running")

  protected def consume(): Unit
  protected def stopped(): Unit = {}

  protected def withStateOrNull[E <: Entity](applyResult: ApplyStateChange.ApplyResult[E])(action: (String, EntityState.Live[E]) => Unit): Unit = {
    applyResult.state.foreach {
      case v: EntityState.Live[E] => action(v.id, v)
      case d: EntityState.Deleted[E] => action(d.id, null)
    }
  }

  protected def produceState[E <: Entity : TypeTag](
    producer: KafkaProducer[String, String],
    topicName: String,
    partitionOffsetString: String,
    timeoutDuration: Duration,
    id: String,
    state: EntityState.Live[E]
  )(implicit encoder: Encoder[EntityState.Live[E]]): Unit = {
    val key = s"${typeOf[E].entityName}#$id"
    producer.retryIfFails(
      p => Await.result(p.produce(topicName, key, state), timeoutDuration),
      beforeRetry = e => {
        logger.error(s"KafkaError while producing $key", e)
        ApplicationMetrics.errorCounter.inc()
      }
    )
    logger.info(s"$partitionOffsetString: produced state $id: $state")
  }

  def stop(): Unit = {
    logger.info("stopping consuming")
    stopConsuming.set(true)
    Await.ready(consumeFuture, Duration.Inf)
    stopped()
    logger.info("stopping stopped")
  }
}

abstract class StateAdaptorStateful(
  changeLogKafkaConnection: KafkaConnection,
  changeLogKafkaConsumerGroup: String
)(implicit ec: ExecutionContextExecutor) extends StateAdaptor {

  protected def handleChangeLogMessage(receivedChangeLogMessage: ReceivedChangeLogMessage, alreadyProcessed: Boolean): Unit
  protected def handleReprocessCompleted(): Unit = {}

  protected override def consume(): Unit = {
    KafkaChangeLogStore.consumeFromBeginningWithAutoCommit(
      KafkaConsumerConfig(changeLogKafkaConnection, Some(changeLogKafkaConsumerGroup)),
      handleChangeLogMessage,
      () => handleReprocessCompleted(),
      stopConsuming,
      logger
    )
  }
}

abstract class StateAdaptorStateless(
  changeLogKafkaConnection: KafkaConnection,
  changeLogKafkaConsumerGroup: String
)(implicit ec: ExecutionContextExecutor) extends StateAdaptor {

  protected def handleChangeLogMessage(receivedChangeLogMessage: ReceivedChangeLogMessage): Unit

  protected override def consume(): Unit = {
    KafkaChangeLogStore.consumeFromStoredWithAutoCommit(
      KafkaConsumerConfig(changeLogKafkaConnection, Some(changeLogKafkaConsumerGroup)),
      handleChangeLogMessage,
      stopConsuming,
      logger
    )
  }
}
