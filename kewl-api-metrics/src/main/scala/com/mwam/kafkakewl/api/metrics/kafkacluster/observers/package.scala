/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster

import com.mwam.kafkakewl.utils._
import nl.grons.metrics4.scala.MetricBuilder

package object observers {
  type CreateGaugeFuncWithName = (String, () => Unit) // don't care about the actual gauge that was created

  /**
   * The possible errors that can come out of the various caches.
   */
  sealed trait CacheError
  object CacheError {
    final case object NoConsumerGroupMetricsForKafkaCluster extends CacheError
    final case object NoConsumerGroupMetricsForConsumerGroup extends CacheError
    final case object NoConsumerGroupMetricsForTopic extends CacheError
    final case object NoTopicInfosForKafkaCluster extends CacheError
    final case object NoTopicInfoForTopic extends CacheError
    final case object NoTopicMetricsForKafkaCluster extends CacheError
    final case object NoTopicMetricsForTopic extends CacheError
  }

  /**
   * The error or a value that can come out of the various caches.
   */
  type CacheResult[T] = Either[CacheError, T]

  implicit class CacheResultExtensions[T](cacheResult: CacheResult[T]) {
    /**
     * Converts the cache result to a result with a string error type.
     * @return the result with the error converted to string
     */
    def toResult: Either[String, T] = cacheResult.left.map(_.toString)
  }

  implicit class MetricBuilderExtensionsForObservers(metrics: MetricBuilder) {
    def createGaugeFuncWithName[A](gaugeName: String, defaultValue: A)(f: => Option[A]): CreateGaugeFuncWithName =
      (
        metrics.metricFullName(gaugeName),
        () => metrics.createGaugeIfDoesNotExistFast(gaugeName, defaultValue)(f)
      )
  }
}
