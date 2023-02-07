/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster.topics

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import com.mwam.kafkakewl.utils._
import com.mwam.kafkakewl.api.metrics.kafkacluster.{KafkaClusterMetricsHelper, TopicInfoObserver}
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics4.scala.{DefaultInstrumented, MetricName}

import scala.concurrent._
import scala.concurrent.duration.Duration

class KafkaTopicMetricsPoller(
  helper: KafkaClusterMetricsHelper,
  observer: TopicInfoObserver,
  delayMillis: Int,
  enabled: Boolean
)(implicit ec: ExecutionContext) extends KafkaTopicMetricsCollector with DefaultInstrumented with MdcUtils with LazyLogging {
  override lazy val metricBaseName = MetricName("com.mwam.kafkakewl.api.metrics.source.topic")

  private val kafkaClusterId = helper.kafkaClusterId
  private val stopCollecting = new AtomicBoolean(false)
  private val pollFuture = Future { blocking { withMDC(helper.mdc)(pollTopicMetrics()) } }
  pollFuture.crashIfFailed()

  private val lastSuccessfulPollTopicNanos = new AtomicLong(0)

  private val topicInfoAgeMillisGaugeName = s"${kafkaClusterId}:topicinfoagemillis"
  private val topicInfoAgeMillisGauge = metrics.gauge(topicInfoAgeMillisGaugeName) {
    durationSince(lastSuccessfulPollTopicNanos.get()).toMillis
  }

  private def pollTopicMetrics(): Unit = {
    while (!stopCollecting.get()) {
      val startNanoTime = System.nanoTime()
      val topicInfosOrError = if (enabled) helper.getAllTopicInfos() else Right((Seq.empty, Seq.empty))
      topicInfosOrError match {
        case Right((topicInfos, missingTopicPartitions)) =>
          lastSuccessfulPollTopicNanos.set(System.nanoTime())
          val durationGetAllTopicInfos = durationSince(startNanoTime)
          val (_, durationUpdate) = durationOf {
            observer.updateAllTopicInfos(kafkaClusterId, topicInfos.map(t => (t.topic, t)).toSortedMap)
          }
          metrics.timer(s"${kafkaClusterId}:getalltopicinfos").update(durationGetAllTopicInfos)
          metrics.timer(s"${kafkaClusterId}:notifyobservers").update(durationUpdate)
          if (missingTopicPartitions.nonEmpty) {
            metrics.meter(s"${kafkaClusterId}:getalltopicinfosmissingtopicpartitions").mark(missingTopicPartitions.size)
            logger.warn(s"missing topic-partition metadata while polling topic infos: ${missingTopicPartitions.toPrettyString}")
          }
          logger.info(s"getAllTopicInfos(${if (enabled) "enabled" else "disabled"}): ${topicInfos.size} topic infos in ${durationGetAllTopicInfos.toMillis} ms - notified observers in ${durationUpdate.toMillis} ms")

        case Left(e) =>
          metrics.meter(s"${kafkaClusterId}:getalltopicinfosfailed").mark()
          ApplicationMetrics.errorCounter.inc()
          logger.error(s"error while polling topic infos: ${e.mkString(", ")}")
      }

      Thread.sleep(delayMillis)
    }
  }

  def stop(): Unit = {
    stopCollecting.set(true)
    Await.result(pollFuture, Duration.Inf)
    metrics.removeGauge(topicInfoAgeMillisGaugeName)
  }
}
