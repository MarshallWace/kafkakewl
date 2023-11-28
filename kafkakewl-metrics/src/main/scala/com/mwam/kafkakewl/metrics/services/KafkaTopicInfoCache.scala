/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.services

import com.mwam.kafkakewl.metrics.domain.KafkaTopicPartitionInfoExtensions.*
import com.mwam.kafkakewl.metrics.domain.{KafkaSingleTopicPartitionInfos, KafkaTopicPartitionInfoChanges}
import zio.*

class KafkaTopicInfoCache(
    private val kafkaTopicInfosRef: Ref[Map[String, KafkaSingleTopicPartitionInfos]]
) {
  def getTopics: UIO[Seq[String]] = kafkaTopicInfosRef.get.map(_.keys.toSeq.sorted)
  def getTopicPartitionInfos(topic: String): UIO[Option[KafkaSingleTopicPartitionInfos]] = kafkaTopicInfosRef.get.map(_.get(topic))
}

object KafkaTopicInfoCache {
  def live: ZLayer[KafkaTopicInfoSource, Nothing, KafkaTopicInfoCache] =
    ZLayer.scoped {
      for {
        topicChangesDequeue <- ZIO.serviceWithZIO[KafkaTopicInfoSource](_.subscribe())
        kafkaTopicInfosRef <- Ref.make(Map.empty[String, KafkaSingleTopicPartitionInfos])
        processTopicChangesFiber <- processTopicChanges(topicChangesDequeue, kafkaTopicInfosRef).forever.fork
        _ <- ZIO.addFinalizer(processTopicChangesFiber.interrupt)
      } yield KafkaTopicInfoCache(kafkaTopicInfosRef)
    }

  private def processTopicChanges(
      topicInfoChangesDequeue: Dequeue[KafkaTopicPartitionInfoChanges],
      topicPartitionInfosRef: Ref[Map[String, KafkaSingleTopicPartitionInfos]]
  ) = for {
    topicInfoChanges <- topicInfoChangesDequeue.take
    _ <- topicPartitionInfosRef.update(_.applyChanges(topicInfoChanges))
    _ <- ZIO.logInfo(
      s"applied ${topicInfoChanges.addedOrUpdated.size} added or updated topic infos and ${topicInfoChanges.removed.size} removed ones"
    )
  } yield ()
}
