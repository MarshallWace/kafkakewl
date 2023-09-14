/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.services

import com.mwam.kafkakewl.metrics.domain.KafkaTopicPartitionInfoExtensions.*
import com.mwam.kafkakewl.metrics.domain.{KafkaTopicPartition, KafkaTopicPartitionInfo, KafkaTopicPartitionInfoChanges, KafkaTopicPartitionInfos}
import org.apache.kafka.common.TopicPartition
import zio.*
import zio.kafka.consumer.Consumer
import zio.stream.*

class KafkaTopicInfoSource(
  private val scope: Scope, // TODO This is slightly dangerous, the scope must not be used after it's closed
  private val topicInfoChangesStream: ZStream[Any, Throwable, KafkaTopicPartitionInfoChanges],
  private val hub: Hub[KafkaTopicPartitionInfoChanges]
) {
  def subscribe(): URIO[Scope, Dequeue[KafkaTopicPartitionInfoChanges]] = hub.subscribe
  def startPublishing(): UIO[Unit] = for {
    topicInfoChangesFiber <- topicInfoChangesStream.runScoped(ZSink.foreach(hub.publish)).provideEnvironment(ZEnvironment(scope)).orDie.fork // TODO error handling!
    // TODO not expecting subscribers after this -> those would not get the initial snapshot of topic infos, only diffs
    // TODO check if we need to interrupt the fiber at all, maybe runScoped does it already?
    _ <- scope.addFinalizer(topicInfoChangesFiber.interrupt *> hub.shutdown)
  } yield ()
}

object KafkaTopicInfoSource {
  def live: ZLayer[Consumer, Nothing, KafkaTopicInfoSource] =
    ZLayer.scoped {
      for {
        scope <- ZIO.service[Scope]
        consumer <- ZIO.service[Consumer]
        hub <- Hub.bounded[KafkaTopicPartitionInfoChanges](requestedCapacity = 1)
      } yield KafkaTopicInfoSource(scope, createKafkaTopicInfosStream(consumer), hub)
    }

  private def createKafkaTopicInfosStream(consumer: Consumer, timeout: Duration = 10.seconds): ZStream[Any, Throwable, KafkaTopicPartitionInfoChanges] = {
    val queryTopicPartitionInfos = (for {
      kafkaTopicPartitionInfos <- consumer.listTopics(timeout)
      topicPartitions = kafkaTopicPartitionInfos
        // TODO because otherwise the requests times out, probably there are some corrupt topics?
        .filter { case (topic, _) => topic.startsWith("kewltest") || topic.startsWith("streambook") || topic.startsWith("dbz") }
        .flatMap { case (topic, kafkaPartitionInfos) => kafkaPartitionInfos.map(tpi => new TopicPartition(topic, tpi.partition)) }
        .toSet
      beginningEndOffsets <- consumer.beginningOffsets(topicPartitions, timeout) zip consumer.endOffsets(topicPartitions, timeout)
    } yield {
      val (beginningOffsets, endOffsets) = beginningEndOffsets
      // TODO should indicate if there are topics for which we don't have either beginning or end-offsets or both
      (beginningOffsets.keySet intersect endOffsets.keySet)
        .view
        .map { tp =>
          (
            KafkaTopicPartition(tp),
            KafkaTopicPartitionInfo(beginningOffsets(tp), endOffsets(tp))
          )
        }
        .toMap
    }).tapError(t => ZIO.logError(s"failed to get the topics' beginning and end offsets: ${t.getMessage}"))
      .fold(_ => None, topicPartitionInfos => Some(topicPartitionInfos))

    ZStream.repeatZIOWithSchedule(queryTopicPartitionInfos.timed, Schedule.fixed(5.seconds))
      // removing the none items because they are failed queries
      .collect { case (duration, Some(topicPartitionInfos)) => (duration, topicPartitionInfos) }
      .tap { case (duration, topicPartitionInfos) => ZIO.logInfo(s"received ${topicPartitionInfos.size} topic-partition beginning-end offsets in ${duration.toMillis / 1000.0} seconds") }
      .map { case (_, topicPartitionInfos) => topicPartitionInfos }
      // putting the previous and current topic infos together, so that we can diff them below and publish the changes only
      .scan((KafkaTopicPartitionInfos.empty, KafkaTopicPartitionInfos.empty)) {
        case ((_, previous), current) => (previous, current)
      }
      // first emitted output of scan (2 empty maps) isn't needed
      .filter { case (previous, current) => previous.nonEmpty || current.nonEmpty }
      // diffing the current to the previous and emitting the changes
      .map { case (previous, current) => previous.diff(current) }
      .tap { changes => ZIO.logInfo(s"emitting ${changes.addedOrUpdated.size} added or updated topic-partition infos and ${changes.removed.size} removed ones")}
  }
}
