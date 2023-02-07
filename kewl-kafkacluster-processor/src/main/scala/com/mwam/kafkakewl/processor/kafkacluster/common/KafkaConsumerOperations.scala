/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.common

import cats.syntax.either._
import com.mwam.kafkakewl.utils._
import com.mwam.kafkakewl.kafka.utils.KafkaExtensions._
import com.mwam.kafkakewl.domain.deploy.{OffsetOfTopicPartition, TopicPartitionPosition, TopicPartitionPositionOfTopicPartition}
import com.mwam.kafkakewl.domain.kafka.KafkaTopic
import com.mwam.kafkakewl.domain.{CommandError, _}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.util.Try

trait KafkaConsumerOperations[K, V] {
  val consumer: KafkaConsumer[K, V]

  def resetConsumerGroupOffsets(topicPartitionOffsets: Iterable[OffsetOfTopicPartition]): ValueOrCommandErrors[Iterable[OffsetOfTopicPartition]] = {
    Try {
      consumer.commitSync(
        topicPartitionOffsets
          .map(tpo => (new TopicPartition(tpo.topic, tpo.partition), tpo.offset))
          .toMap
      )
      topicPartitionOffsets
    }.toRightOrCommandErrors
  }

  def resetConsumerGroupOffsets(
    topicPartitionPositions: Iterable[TopicPartitionPositionOfTopicPartition],
    dryRun: Boolean
  ): ValueOrCommandErrors[Seq[OffsetOfTopicPartition]] =
    for {
      topicPartitionsOffsets <- calculateOffsets(topicPartitionPositions)
      _ <- if (dryRun) ().asRight[Seq[CommandError]] else resetConsumerGroupOffsets(topicPartitionsOffsets)
    } yield topicPartitionsOffsets

  def ensureAllTopicPartitionsAreThere(
    topicPartitions: Set[TopicPartition],
    topicPartitionOffsets: Map[TopicPartition, Long]
  ): ValueOrCommandErrors[Map[TopicPartition, Long]] = {
    val missingTopicPartitionErrors = topicPartitions
      .filter(!topicPartitionOffsets.contains(_))
      .map(tp => CommandError.otherError(s"missing topic='${tp.topic}',partition=${tp.partition}"))
      .toSeq
    if (missingTopicPartitionErrors.nonEmpty)
      missingTopicPartitionErrors.asLeft[Map[TopicPartition, Long]]
    else
      // just making sure we don't have more topic-partition offsets (probably can't happen, but who knows with kafka)
      topicPartitionOffsets.filterKeys(topicPartitions.contains).asRight
  }

  def collectTopicPartitionsWithAndWithoutOffsets(
    topicPartitions: Set[TopicPartition],
    topicPartitionOffsets: Map[TopicPartition, Long]
  ): (Map[TopicPartition, Long], Seq[TopicPartition]) = {
    // just making sure we don't have more topic-partition offsets (probably can't happen, but who knows with kafka)
    val topicPartitionOffsetsFiltered = topicPartitionOffsets.filterKeys(topicPartitions.contains)
    val missingTopicPartitions = topicPartitions.filter(!topicPartitionOffsets.contains(_)).toSeq
    (topicPartitionOffsetsFiltered, missingTopicPartitions)
  }

  def calculateOffsets(
    topicPartitionPositions: Iterable[TopicPartitionPositionOfTopicPartition]
  ): ValueOrCommandErrors[Seq[OffsetOfTopicPartition]] = {
    val positionByTopicPartition = topicPartitionPositions.map(tpp => (new TopicPartition(tpp.topic, tpp.partition), tpp.position)).toMap

    for {
      beginningOffsets <- Try(consumer.beginningOffsets(positionByTopicPartition.keys)).toRightOrCommandErrors
      endOffsetsOfError <- Try(consumer.endOffsets(positionByTopicPartition.keys)).toRightOrCommandErrors

      beginningOffsets <- ensureAllTopicPartitionsAreThere(positionByTopicPartition.keySet, beginningOffsets)
      endOffsets <- ensureAllTopicPartitionsAreThere(positionByTopicPartition.keySet, endOffsetsOfError)

      topicPartitionOffsets = positionByTopicPartition.map { case (tp, position) =>
        val offset = position match {
          // We shouldn't have TopicPartitionPosition.Default here, but if it happens, just use TopicPartitionPosition.Beginning()
          case TopicPartitionPosition.Default() | TopicPartitionPosition.Beginning() => beginningOffsets(tp).asRight[CommandError]
          case TopicPartitionPosition.End() => endOffsets(tp).asRight[CommandError]
          case TopicPartitionPosition.Offset(o) => math.max(math.min(o, endOffsets(tp)), beginningOffsets(tp)).asRight[CommandError]
          case TopicPartitionPosition.TimeStamp(ts) =>
            for {
              offsets <- Try(consumer.offsetsForTimes(Map(tp -> ts))).toRightOrCommandError
            } yield offsets.getOrElse(tp, endOffsets(tp)) // if there is no offset returned, we use the latest (no message with timestamp later or equal to the one here)
        }
        (tp, offset)
      }

      // fail-fast if there was any error calculating any of the offsets
      topicPartitionOffsetsErrors = topicPartitionOffsets.collect { case (_, Left(ce)) => ce }.toSeq
      _ <- Either.cond(topicPartitionOffsetsErrors.isEmpty, (), topicPartitionOffsetsErrors)
    } yield topicPartitionOffsets
      .collect { case (tp, Right(o)) => OffsetOfTopicPartition(tp.topic, tp.partition, o) }
      .toSeq
  }

  def partitionsOfTopics(topics: Seq[String]): ValueOrCommandError[Map[String, Int]] = {
    for {
      topicPartitions <- Try(consumer.partitionsOf(topics.toSet)).toRightOrCommandError
      topicPartitionsMap = topicPartitions.groupBy(_.topic).mapValues(_.map(_.partition).max + 1)
      topicsWithNoPartitions = topics.filter(topic => !topicPartitionsMap.contains(topic))
      _ <- Either.cond(topicsWithNoPartitions.isEmpty, (), CommandError.otherError(s"topics '${topicsWithNoPartitions.map(_.quote).mkString(", ")}' doesn't have any partitions"))
    } yield topicPartitionsMap
  }

  def getTopicsInfo(topicNames: Seq[String]): ValueOrCommandErrors[(Seq[KafkaTopic.Info], Seq[KafkaTopic.Partition])] = {
    for {
      numberOfTopicsPartitions <- partitionsOfTopics(topicNames).toCommandErrors
      topicPartitions = numberOfTopicsPartitions
        .flatMap { case (topicName, numberOfPartitions) => (0 until numberOfPartitions).map(p => new TopicPartition(topicName, p)) }
      topicPartitionsSet = topicPartitions.toSet

      beginningOffsets <- Try(consumer.beginningOffsets(topicPartitions)).toRightOrCommandErrors
      endOffsets <- Try(consumer.endOffsets(topicPartitions)).toRightOrCommandErrors
    } yield {
      // finding all the missing beginning or end offsets
      val (filteredBeginningOffsets, missingTopicPartitionsFromBeginningOffsets) = collectTopicPartitionsWithAndWithoutOffsets(topicPartitionsSet, beginningOffsets)
      val (filteredEndOffsets, missingTopicPartitionsFromEndOffsets) = collectTopicPartitionsWithAndWithoutOffsets(topicPartitionsSet, endOffsets)
      val missingTopicPartitions =
        (missingTopicPartitionsFromBeginningOffsets ++ missingTopicPartitionsFromEndOffsets)
          .distinct
          .map(KafkaTopic.Partition(_))

      // building the topic-infos from the available offsets
      val topicInfos = topicNames.map { topicName =>
        KafkaTopic.Info(
          topicName,
          topicPartitions
            .filter(_.topic == topicName)
            // need to skip any unavailable beginning/end offsets
            .flatMap { tp =>
              for {
                beginningOffset <- filteredBeginningOffsets.get(tp)
                endOffset <- filteredEndOffsets.get(tp)
              } yield (tp.partition, KafkaTopic.PartitionInfo(beginningOffset, endOffset))
            },
          None
        )
      }

      (topicInfos, missingTopicPartitions)
    }
  }

  def getTopicsInfoFailFast(topicNames: Seq[String]): ValueOrCommandErrors[Seq[KafkaTopic.Info]] = {
    // like getTopicsInfo, but fails-fast on any missing offsets
    getTopicsInfo(topicNames)
      .flatMap { case (topicInfos, missingTopicPartitions) =>
        if (missingTopicPartitions.nonEmpty) {
          missingTopicPartitions.map(tp => CommandError.otherError(s"missing topic='${tp.topic}',partition=${tp.partition}")).asLeft
        } else {
          topicInfos.asRight
        }
      }
  }

  def partitionsOfTopic(topicName: String): ValueOrCommandError[Int] = partitionsOfTopics(Seq(topicName)).map(_(topicName))
  def getTopicInfo(topicName: String): ValueOrCommandErrors[KafkaTopic.Info] = getTopicsInfoFailFast(Seq(topicName)).map(_.head)
}

trait KafkaConsumerExtraSyntax {
  implicit class KafkaConsumerExtraSyntaxExtensions[K, V](val consumer: KafkaConsumer[K, V]) extends KafkaConsumerOperations[K, V]
}
