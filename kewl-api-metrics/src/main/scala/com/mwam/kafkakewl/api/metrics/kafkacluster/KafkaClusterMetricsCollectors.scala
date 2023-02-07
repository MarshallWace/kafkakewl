/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster

import com.mwam.kafkakewl.domain.Mdc
import com.mwam.kafkakewl.kafka.utils.KafkaConnection
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.utils.{ApplicationMetrics, MdcUtils}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor

/**
  * A set of kafka metrics collectors.
  *
  * NOT thread-safe! Do not call it concurrently!
  */
class KafkaClusterMetricsCollectors(
  observers: KafkaClusterMetricsCollector.Observers,
  config: KafkaClusterMetricsCollector.Config
)(implicit ec: ExecutionContextExecutor) extends LazyLogging with MdcUtils {
  private val collectors = mutable.Map.empty[KafkaClusterEntityId, KafkaClusterMetricsCollector]

  private def createCollectorInternal(kafkaClusterId: KafkaClusterEntityId, connection: KafkaConnection): Unit = {
    collectors += (kafkaClusterId -> new KafkaClusterMetricsCollector(kafkaClusterId, connection, observers, config))
  }

  private def removeCollectorInternal(kafkaClusterId: KafkaClusterEntityId): Unit = {
    collectors(kafkaClusterId).close()
    collectors.remove(kafkaClusterId)
  }

  def createCollector(kafkaClusterId: KafkaClusterEntityId, connection: KafkaConnection): Unit = {
    withMDC(Mdc.fromKafkaClusterId(kafkaClusterId)) {
      val methodLogString = s"createCollector($kafkaClusterId, $connection)"
      logger.info(s"$methodLogString")
      if (collectors.contains(kafkaClusterId)) {
        ApplicationMetrics.errorCounter.inc()
        logger.error(s"$methodLogString: a collector already exists for this kafka-cluster id")
        // it shouldn't happen, but let's try to deal with it
        updateCollector(kafkaClusterId, connection)
      } else {
        createCollectorInternal(kafkaClusterId, connection)
      }
    }
  }

  def updateCollector(kafkaClusterId: KafkaClusterEntityId, connection: KafkaConnection): Unit = {
    withMDC(Mdc.fromKafkaClusterId(kafkaClusterId)) {
      val methodLogString = s"updateCollector($kafkaClusterId, $connection)"
      logger.info(s"$methodLogString")
      if (!collectors.contains(kafkaClusterId)) {
        ApplicationMetrics.errorCounter.inc()
        logger.error(s"$methodLogString: collector doesn't exist for this kafka-cluster id")
        // it shouldn't happen, but let's try to deal with it
        createCollector(kafkaClusterId, connection)
      } else {
        // update is just remove-create (in case any of the connection parameters changed too)
        removeCollectorInternal(kafkaClusterId)
        createCollectorInternal(kafkaClusterId, connection)
      }
    }
  }

  def removeCollector(kafkaClusterId: KafkaClusterEntityId): Unit = {
    withMDC(Mdc.fromKafkaClusterId(kafkaClusterId)) {
      val methodLogString = s"removeCollector($kafkaClusterId)"
      logger.info(s"$methodLogString")
      if (!collectors.contains(kafkaClusterId)) {
        ApplicationMetrics.errorCounter.inc()
        logger.error(s"$methodLogString: collector doesn't exist for this kafka-cluster id")
        // it shouldn't happen, but let's try to deal with it
      } else {
        removeCollectorInternal(kafkaClusterId)
      }
    }
  }

  def close(): Unit = collectors.keys.toSeq.foreach(removeCollector)
}
