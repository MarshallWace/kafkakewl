/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster.observers

import java.time.OffsetDateTime

import cats.syntax.option._
import com.mwam.kafkakewl.api.metrics.kafkacluster.observers.TopicMetricsCache.MetricsWithTimestamp
import com.mwam.kafkakewl.utils._
import com.mwam.kafkakewl.domain.kafka.KafkaTopic
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import org.scalatest.{Matchers, WordSpec}

import scala.collection.SortedMap
import scala.concurrent.duration._

class TopicMetricsCacheSpec extends WordSpec with Matchers {
  private def topicMetricsCache(windowDuration: Duration =  1.minute) = new TopicMetricsCacheImpl(windowDuration)
  private def ts(isoString: String): OffsetDateTime = OffsetDateTime.parse(isoString + "+00:00")

  private val testCluster = KafkaClusterEntityId("test-cluster")
  private val stagingCluster = KafkaClusterEntityId("staging-cluster")
  private val prodCluster = KafkaClusterEntityId("prod-cluster")

  implicit class KafkaTopicInfoExtensions(topicInfo: KafkaTopic.Info) {
    def p(partition: Int, highOffset: Long = 0, lowOffset: Long = 0): KafkaTopic.Info =
      topicInfo.copy(partitions = topicInfo.partitions + (partition -> KafkaTopic.PartitionInfo(lowOffset, highOffset)))
  }

  private val testTopic = "test"

  private def topic(topic: String = testTopic): KafkaTopic.Info =
    KafkaTopic.Info(topic, SortedMap.empty)

  private def topics(topics: KafkaTopic.Info*): SortedMap[String, KafkaTopic.Info] =
    topics.map(t => (t.topic, t)).toSortedMap

  "the cache" when {
    "it is empty" should {
      val tmc = topicMetricsCache()
      "return no kafka-clusters" in { tmc.getKafkaClusterIds shouldBe Right(Iterable.empty) }
      "return no topics" in {
        tmc.getTopics(stagingCluster) shouldBe Left(CacheError.NoTopicMetricsForKafkaCluster)
        tmc.getTopics(prodCluster) shouldBe Left(CacheError.NoTopicMetricsForKafkaCluster)
      }
      "return no topic metrics" in {
        tmc.getTopicMetrics(stagingCluster, testTopic) shouldBe Left(CacheError.NoTopicMetricsForKafkaCluster)
        tmc.getTopicMetrics(prodCluster, testTopic) shouldBe Left(CacheError.NoTopicMetricsForKafkaCluster)
      }
    }
    "it has a single topic info" should {
      val tmc = topicMetricsCache()
      tmc.updateAllTopicInfos(ts("2018-03-12T08:01:23"), prodCluster, topics(topic().p(0, 1)))
      "return a single kafka-cluster" in { tmc.getKafkaClusterIds shouldBe Right(Iterable(prodCluster)) }
      "return a single topic" in { tmc.getTopics(prodCluster) shouldBe Right(Set(testTopic)) }

      // a single data-point can't generate metrics (need at least two, with different time-stamps)
      "return no topic metrics" in { tmc.getTopicMetrics(prodCluster, testTopic) shouldBe Left(CacheError.NoTopicMetricsForTopic) }
    }
    "it has two topic info" should {
      val tmc = topicMetricsCache()
      tmc.updateAllTopicInfos(ts("2018-03-12T08:01:23"), prodCluster, topics(topic().p(0, 1)))
      tmc.updateAllTopicInfos(ts("2018-03-12T08:01:33"), prodCluster, topics(topic().p(0, 11)))
      "return a single kafka-cluster" in { tmc.getKafkaClusterIds shouldBe Right(Iterable(prodCluster)) }
      "return a single topic" in { tmc.getTopics(prodCluster) shouldBe Right(Set(testTopic)) }
      "return the topic metrics" in {
        tmc.getTopicMetrics(prodCluster, testTopic) shouldBe Right(
          MetricsWithTimestamp(
            KafkaTopic.Metrics(1.0, ts("2018-03-12T08:01:33")).some,
            SortedMap(0 -> KafkaTopic.Metrics(1.0, ts("2018-03-12T08:01:33")))
          )
        )
      }
    }
    "it has two topic info and the older one is JUST on the edge of the window" should {
      val tmc = topicMetricsCache()
      tmc.updateAllTopicInfos(ts("2018-03-12T08:01:23"), prodCluster, topics(topic().p(0, 1)))
      tmc.updateAllTopicInfos(ts("2018-03-12T08:02:23"), prodCluster, topics(topic().p(0, 61)))
      "return the topic metrics" in {
        tmc.getTopicMetrics(prodCluster, testTopic) shouldBe Right(
          MetricsWithTimestamp(
            KafkaTopic.Metrics(1.0, ts("2018-03-12T08:02:23")).some,
            SortedMap(0 -> KafkaTopic.Metrics(1.0, ts("2018-03-12T08:02:23")))
          )
        )
      }
    }
    "it has two topic info but the older one is outside of the window" should {
      val tmc = topicMetricsCache()
      tmc.updateAllTopicInfos(ts("2018-03-12T08:01:23"), prodCluster, topics(topic().p(0, 1)))
      tmc.updateAllTopicInfos(ts("2018-03-12T08:02:23.001"), prodCluster, topics(topic().p(0, 11)))
      // there remains only a single data-point which doesn't generate metrics
      "return no topic metrics" in { tmc.getTopicMetrics(prodCluster, testTopic) shouldBe Left(CacheError.NoTopicMetricsForTopic) }
    }
    "it has a few topic info, some scrolls out of the window" should {
      val tmc = topicMetricsCache(5.seconds)
      tmc.updateAllTopicInfos(ts("2018-03-12T08:01:23"), prodCluster, topics(topic().p(0, 1)))
      tmc.updateAllTopicInfos(ts("2018-03-12T08:02:24"), prodCluster, topics(topic().p(0, 2)))
      tmc.updateAllTopicInfos(ts("2018-03-12T08:02:25"), prodCluster, topics(topic().p(0, 3)))
      tmc.updateAllTopicInfos(ts("2018-03-12T08:02:26"), prodCluster, topics(topic().p(0, 4)))
      tmc.updateAllTopicInfos(ts("2018-03-12T08:02:27"), prodCluster, topics(topic().p(0, 5)))
      tmc.updateAllTopicInfos(ts("2018-03-12T08:02:28"), prodCluster, topics(topic().p(0, 6)))
      tmc.updateAllTopicInfos(ts("2018-03-12T08:02:29"), prodCluster, topics(topic().p(0, 6)))
      tmc.updateAllTopicInfos(ts("2018-03-12T08:02:30"), prodCluster, topics(topic().p(0, 13)))
      "return the topic metrics" in {
        tmc.getTopicMetrics(prodCluster, testTopic) shouldBe Right(
          MetricsWithTimestamp(
            KafkaTopic.Metrics(2.0, ts("2018-03-12T08:02:30")).some,
            SortedMap(0 -> KafkaTopic.Metrics(2.0, ts("2018-03-12T08:02:30")))
          )
        )
      }
    }
    "it has a few topic info for a multi-partition topic, some scrolls out of the window" should {
      val tmc = topicMetricsCache(5.seconds)
      tmc.updateAllTopicInfos(ts("2018-03-12T08:01:23"), prodCluster, topics(topic().p(0, 1).p(1, 0)))
      tmc.updateAllTopicInfos(ts("2018-03-12T08:02:24"), prodCluster, topics(topic().p(0, 2).p(1, 10)))
      tmc.updateAllTopicInfos(ts("2018-03-12T08:02:25"), prodCluster, topics(topic().p(0, 3).p(1, 10)))
      tmc.updateAllTopicInfos(ts("2018-03-12T08:02:26"), prodCluster, topics(topic().p(0, 4).p(1, 10)))
      tmc.updateAllTopicInfos(ts("2018-03-12T08:02:27"), prodCluster, topics(topic().p(0, 5).p(1, 10)))
      tmc.updateAllTopicInfos(ts("2018-03-12T08:02:28"), prodCluster, topics(topic().p(0, 6).p(1, 10)))
      tmc.updateAllTopicInfos(ts("2018-03-12T08:02:29"), prodCluster, topics(topic().p(0, 6).p(1, 12)))
      tmc.updateAllTopicInfos(ts("2018-03-12T08:02:30"), prodCluster, topics(topic().p(0, 13).p(1, 13)))
      "return the topic metrics" in {
        tmc.getTopicMetrics(prodCluster, testTopic) shouldBe Right(
          MetricsWithTimestamp(
            KafkaTopic.Metrics(2.6, ts("2018-03-12T08:02:30")).some,
            SortedMap(0 -> KafkaTopic.Metrics(2.0, ts("2018-03-12T08:02:30")), 1 -> KafkaTopic.Metrics(0.6, ts("2018-03-12T08:02:30")))
          )
        )
      }
    }
  }
}
