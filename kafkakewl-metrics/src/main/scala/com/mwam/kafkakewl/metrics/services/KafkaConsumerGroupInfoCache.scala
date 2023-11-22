/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.services

import com.mwam.kafkakewl.metrics.domain.KafkaConsumerGroupInfoExtensions.*
import com.mwam.kafkakewl.metrics.domain.{
  KafkaConsumerGroupInfo,
  KafkaConsumerGroupOffsets
}
import zio.*

class KafkaConsumerGroupInfoCache(
    private val consumerGroupInfosRef: Ref[Map[String, KafkaConsumerGroupInfo]]
) {
  def getConsumerGroups: UIO[Seq[String]] =
    consumerGroupInfosRef.get.map(_.keys.toSeq.sorted)
  def getConsumerGroupInfo(
      consumerGroup: String
  ): UIO[Option[KafkaConsumerGroupInfo]] =
    consumerGroupInfosRef.get.map(_.get(consumerGroup))
}

object KafkaConsumerGroupInfoCache {
  def live
      : ZLayer[ConsumerOffsetsSource, Nothing, KafkaConsumerGroupInfoCache] =
    ZLayer.scoped {
      for {
        consumerGroupOffsetsDequeue <- ZIO
          .serviceWithZIO[ConsumerOffsetsSource](_.subscribe())
        consumerGroupInfosRef <- Ref.make(
          Map.empty[String, KafkaConsumerGroupInfo]
        )
        processConsumerGroupOffsetsFiber <- processConsumerGroupOffsets(
          consumerGroupOffsetsDequeue,
          consumerGroupInfosRef
        ).forever.fork
        _ <- ZIO.addFinalizer(processConsumerGroupOffsetsFiber.interrupt)
      } yield KafkaConsumerGroupInfoCache(consumerGroupInfosRef)
    }

  private def processConsumerGroupOffsets(
      consumerGroupOffsetsDequeue: Dequeue[KafkaConsumerGroupOffsets],
      consumerGroupInfosRef: Ref[Map[String, KafkaConsumerGroupInfo]]
  ) = for {
    consumerGroupOffsets <- consumerGroupOffsetsDequeue.take
    _ <- consumerGroupInfosRef.update(_.applyChanges(consumerGroupOffsets))
    _ <- ZIO.logInfo(
      s"applied ${consumerGroupOffsets.size} consumer group topic partition offsets"
    )
  } yield ()
}
