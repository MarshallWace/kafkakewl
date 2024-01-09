/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.services

import com.mwam.kafkakewl.metrics.domain.{
  ConsumerGroupStatus,
  ConsumerGroupTopicPartition,
  KafkaConsumerGroupMetrics,
  KafkaConsumerGroupOffset,
  KafkaConsumerGroupOffsets,
  KafkaTopicPartition,
  KafkaTopicPartitionInfo,
  KafkaTopicPartitionInfoChanges
}
import zio.{Dequeue, Duration, Fiber, Hub, Queue, Schedule, Scope, UIO, URIO, ZEnvironment, ZIO, ZLayer}
import zio.stream.{ZSink, ZStream}

case class KafkaConsumerGroupMetricChanges(
    addedOrUpdated: Map[ConsumerGroupTopicPartition, KafkaConsumerGroupMetrics],
    removed: Set[ConsumerGroupTopicPartition]
)

case class ConsumerGroupTopicPartitionMetricsWorker(
    fibreHandle: Fiber[Nothing, Unit],
    partitionInfoUpdate: Queue[KafkaTopicPartitionInfo],
    consumerGroupUpdate: Queue[KafkaConsumerGroupOffset]
)

class ConsumerGroupMetricsManager(
    val workerOutputQueue: Queue[KafkaConsumerGroupMetricChanges],
    val workers: Map[ConsumerGroupTopicPartition, ConsumerGroupTopicPartitionMetricsWorker] = Map.empty,
    val partitionConsumers: Map[KafkaTopicPartition, Set[ConsumerGroupTopicPartition]] = Map.empty,
    val latestTopicPartitionInfos: Map[KafkaTopicPartition, KafkaTopicPartitionInfo] = Map.empty,
    val latestConsumerGroupOffsets: Map[ConsumerGroupTopicPartition, KafkaConsumerGroupOffset] = Map.empty
) {

  def processKafkaConsumerGroupOffsets(groupOffsets: KafkaConsumerGroupOffsets): UIO[ConsumerGroupMetricsManager] = {
    // Split groupOffsets by None-valued and Some-valued
    val addedOrUpdated = groupOffsets.flatMap((k, v) => Some(k).zip(v))
    val removed = groupOffsets.filter((_, v) => v.isEmpty).keySet
    // Update the cache
    val (newLatestConsumerGroupOffsets, addedKeys, removedGroupTopicPartition) =
      ConsumerGroupMetricsManager.updateMapWithAddedRemoved(latestConsumerGroupOffsets, addedOrUpdated, removed)
    for {

      // Push the updates to current workers
      _ <- ZIO.foreachParDiscard(addedOrUpdated) { (groupTopicPartition, groupOffset) =>
        workers
          .get(groupTopicPartition)
          .map(worker => worker.consumerGroupUpdate.offer(groupOffset))
          .getOrElse(ZIO.unit)
      }

      // Create new workers for new ConsumerGroupTopicPartition
      addedWorkers <- ZIO.foreachPar(addedKeys) { consumerGroupTopicPartition =>
        ZIO
          .succeed(consumerGroupTopicPartition)
          .zip(createNewWorkerFiber(consumerGroupTopicPartition, latestTopicPartitionInfos, newLatestConsumerGroupOffsets))
      }

      workersWithAdded = workers ++ addedWorkers

      // Update partitionConsumers to reflect new and removed consumer groups
      newPartitionConsumers = ConsumerGroupMetricsManager.updateMapOfSetsWithAddedRemoved(
        partitionConsumers,
        addedOrUpdated.map((cgtp, _) => (cgtp.topicPartition, cgtp)),
        removed.map(cgtp => (cgtp.topicPartition, cgtp)).toMap
      )

      // Remove workers that have had nulls pushed to them, interrupting their fibres
      _ <- ZIO.foreachParDiscard(removedGroupTopicPartition) { groupTopicPartition =>
        workersWithAdded.get(groupTopicPartition).map(worker => worker.fibreHandle.interrupt).getOrElse(ZIO.unit) *> workerOutputQueue.offer(
          KafkaConsumerGroupMetricChanges(Map.empty, Set(groupTopicPartition))
        )
      }
      workersWithRemoved = workersWithAdded.removedAll(removedGroupTopicPartition)

    } yield ConsumerGroupMetricsManager(
      workerOutputQueue,
      workersWithRemoved,
      newPartitionConsumers,
      latestTopicPartitionInfos,
      newLatestConsumerGroupOffsets
    )
  }

  private def createNewWorkerFiber(
      consumerGroupTopicPartition: ConsumerGroupTopicPartition,
      latestTopicPartitionInfos: Map[KafkaTopicPartition, KafkaTopicPartitionInfo],
      latestConsumerGroupOffsets: Map[ConsumerGroupTopicPartition, KafkaConsumerGroupOffset]
  ): UIO[ConsumerGroupTopicPartitionMetricsWorker] = {
    for {
      partitionInfoUpdate <- Queue.unbounded[KafkaTopicPartitionInfo]
      _ <- ZIO.foreachDiscard(latestTopicPartitionInfos.get(consumerGroupTopicPartition.topicPartition))(topicPartitionInfo =>
        partitionInfoUpdate.offer(topicPartitionInfo)
      )
      consumerGroupUpdate <- Queue.unbounded[KafkaConsumerGroupOffset]
      _ <- ZIO.foreachDiscard(latestConsumerGroupOffsets.get(consumerGroupTopicPartition))(consumerGroupOffset =>
        consumerGroupUpdate.offer(consumerGroupOffset)
      )

      workerFiber <- ZStream
        .fromQueueWithShutdown(partitionInfoUpdate)
        .mergeEither(ZStream.fromQueueWithShutdown(consumerGroupUpdate))
        .runFoldZIO((Option.empty[KafkaTopicPartitionInfo], Option.empty[KafkaConsumerGroupOffset])) { (state, update) =>
          // TODO: The window metric functions
          val (newState) = update match {
            case Left(kafkaTopicPartitionInfo)   => (Some(kafkaTopicPartitionInfo), state._2)
            case Right(kafkaConsumerGroupOffset) => (state._1, Some(kafkaConsumerGroupOffset))
          }
          workerOutputQueue
            .offer(
              KafkaConsumerGroupMetricChanges(
                Map(
                  consumerGroupTopicPartition -> KafkaConsumerGroupMetrics(
                    ConsumerGroupStatus.Ok,
                    newState._1,
                    newState._2,
                    newState._1.zip(newState._2).map((tpi, cgo) => (tpi.endOffset - cgo.offset).max(0)),
                    Some(0.0)
                  )
                ),
                Set.empty
              )
            )
            .as(newState)
        }
        .ignore
        .fork
    } yield ConsumerGroupTopicPartitionMetricsWorker(workerFiber, partitionInfoUpdate, consumerGroupUpdate)
  }

  def processKafkaTopicPartitionInfoChanges(topicPartitionInfoChanges: KafkaTopicPartitionInfoChanges): UIO[ConsumerGroupMetricsManager] = {
    val addedOrUpdated = topicPartitionInfoChanges.addedOrUpdated
    val removed = topicPartitionInfoChanges.removed
    // Update the cache
    val (newTopicPartitionInfos, _, _) =
      ConsumerGroupMetricsManager.updateMapWithAddedRemoved(latestTopicPartitionInfos, addedOrUpdated, removed)

    // Push the updates to current workers
    val updatePushes = ZIO.foreachParDiscard(addedOrUpdated.flatMap { (topicPartition, topicPartitionInfo) =>
      partitionConsumers
        .get(topicPartition)
        .toSet
        .flatten
        .map((_, topicPartitionInfo))
    }) { (groupTopicPartition, topicPartitionInfo) =>
      workers.get(groupTopicPartition).map(worker => worker.partitionInfoUpdate.offer(topicPartitionInfo)).getOrElse(ZIO.unit)
    }

    // Interrupt workers whose topic partitions have been removed, remove them from the worker map
    val workerTerminations = ZIO.foreachParDiscard(removed.flatMap { topicPartition =>
      partitionConsumers
        .get(topicPartition)
        .toSet
        .flatten
    }) { groupTopicPartition =>
      workers.get(groupTopicPartition).map(worker => worker.fibreHandle.interrupt).getOrElse(ZIO.unit) *> workerOutputQueue.offer(
        KafkaConsumerGroupMetricChanges(Map.empty, Set(groupTopicPartition))
      )
    }
    val workersWithRemoved = workers.removedAll(removed.flatMap { topicPartition =>
      partitionConsumers
        .get(topicPartition)
        .toSet
    }.flatten)

    // Update partitionConsumers to remove deleted partitions
    val newPartitionConsumers = partitionConsumers.removedAll(removed)

    updatePushes *> workerTerminations.as(
      ConsumerGroupMetricsManager(workerOutputQueue, workersWithRemoved, newPartitionConsumers, newTopicPartitionInfos, latestConsumerGroupOffsets)
    )
  }
}

object ConsumerGroupMetricsManager {
  // Returns tuple of new map, new keys, removed keys
  def updateMapWithAddedRemoved[K, V](oldMap: Map[K, V], addedOrUpdated: Map[K, V], removed: Set[K]): (Map[K, V], Set[K], Set[K]) = {
    val newMap =
      oldMap.keySet.union(addedOrUpdated.keySet).map(key => (key, oldMap.get(key).orElse(addedOrUpdated.get(key)).get)).toMap.removedAll(removed)
    val newKeys = addedOrUpdated.keySet.diff(oldMap.keySet)
    val removedKeys = oldMap.keySet.intersect(removed)
    (newMap, newKeys, removedKeys)
  }

  def updateMapOfSetsWithAddedRemoved[K, V](oldMap: Map[K, Set[V]], addedOrUpdated: Map[K, V], removed: Map[K, V]): Map[K, Set[V]] = {
    val newMap =
      oldMap.keySet
        .union(addedOrUpdated.keySet)
        .map(key => (key, oldMap(key).union(addedOrUpdated.get(key).toSet).removedAll(removed.get(key))))
        .toMap
    newMap
  }
}

class KafkaConsumerGroupMetricsCalc(
    private val scope: Scope, // TODO This is slightly dangerous, the scope must not be used after it's closed
    private val consumerGroupMetricsStream: ZStream[Any, Throwable, KafkaConsumerGroupMetricChanges],
    private val hub: Hub[KafkaConsumerGroupMetricChanges]
) {
  def subscribe(): URIO[Scope, Dequeue[KafkaConsumerGroupMetricChanges]] = hub.subscribe
  def startPublishing(): UIO[Unit] = for {
    consumerGroupMetricsFiber <- consumerGroupMetricsStream
      .runScoped(ZSink.foreach(hub.publish))
      .provideEnvironment(ZEnvironment(scope))
      .orDie
      .fork // TODO error handling!
    // TODO not expecting subscribers after this -> those would not get the initial snapshot of topic infos, only diffs
    // TODO check if we need to interrupt the fiber at all, maybe runScoped does it already?
    _ <- scope.addFinalizer(consumerGroupMetricsFiber.interrupt *> hub.shutdown *> ZIO.log("KafkaConsumerGroupMetricsCalc has shut down"))
  } yield ()
}

object KafkaConsumerGroupMetricsCalc {

  def live: ZLayer[ConsumerOffsetsSource & KafkaTopicInfoSource, Nothing, KafkaConsumerGroupMetricsCalc] =
    ZLayer.scoped {
      for {
        scope <- ZIO.service[Scope]
        consumerOffsetSource <- ZIO.service[ConsumerOffsetsSource]
        consumerOffsetStream <- consumerOffsetSource.subscribe().map(ZStream.fromQueueWithShutdown(_))
        topicInfoSource <- ZIO.service[KafkaTopicInfoSource]
        topicInfoStream <- topicInfoSource.subscribe().map(ZStream.fromQueueWithShutdown(_))
        hub <- Hub.bounded[KafkaConsumerGroupMetricChanges](requestedCapacity = 1)
        groupMetricsStream <- createConsumerGroupMetricStream(consumerOffsetStream, topicInfoStream)
      } yield KafkaConsumerGroupMetricsCalc(scope, groupMetricsStream, hub)
    }

  private def createConsumerGroupMetricStream(
      consumerOffsetStream: ZStream[Any, Nothing, KafkaConsumerGroupOffsets],
      topicInfoStream: ZStream[Any, Nothing, KafkaTopicPartitionInfoChanges]
  ): UIO[ZStream[Any, Throwable, KafkaConsumerGroupMetricChanges]] = for {
    // Manager loop merges the two relevant streams and processes each message individually in a fold
    outputQueue <- Queue.unbounded[KafkaConsumerGroupMetricChanges]
    _ <- consumerOffsetStream
      .mergeEither(topicInfoStream)
      .runFoldZIO(new ConsumerGroupMetricsManager(outputQueue)) { (manager, update) =>
        update match {
          case Left(kafkaConsumerGroupOffsets)       => manager.processKafkaConsumerGroupOffsets(kafkaConsumerGroupOffsets)
          case Right(kafkaTopicPartitionInfoChanges) => manager.processKafkaTopicPartitionInfoChanges(kafkaTopicPartitionInfoChanges)
        }
      }
      .fork
  } yield ZStream
    .fromQueueWithShutdown(outputQueue)
    .aggregateAsyncWithin(ZSink.collectAll[KafkaConsumerGroupMetricChanges], Schedule.fixed(Duration.fromSeconds(1)))
    .map { updates =>

      val addedOrUpdated =
        updates.map(_.addedOrUpdated).foldLeft(Map.empty[ConsumerGroupTopicPartition, KafkaConsumerGroupMetrics]) { (accum, update) =>
          accum ++ update
        }
      val removed = updates.map(_.removed).foldLeft(Set.empty[ConsumerGroupTopicPartition]) { (accum, update) =>
        accum ++ update
      }
      KafkaConsumerGroupMetricChanges(addedOrUpdated, removed)
    }
}
