/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.common

import scala.collection.JavaConverters._
import cats.syntax.traverse._
import cats.instances.vector._
import cats.data.Validated._
import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.kafka.utils.KafkaExtensions._
import com.mwam.kafkakewl.domain.kafka._
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

trait KafkaAdminClientAndConsumerOperations[K, V] {
  this: KafkaAdminClientOperations with KafkaConsumerOperations[K, V] =>

  val adminClient: AdminClient
  val consumer: KafkaConsumer[K, V]

  def getConsumerGroupInfo(consumerGroupId: String): ValueOrCommandErrors[KafkaConsumerGroup.Info] = for {
    // getting the description and offsets for the consumer group
    cgd <- adminClient.describeConsumerGroup(consumerGroupId).toRightOrCommandErrors
    cgo <- adminClient.consumerGroupOffsets(consumerGroupId).toRightOrCommandErrors

    // getting the topic info for all topics that it has offsets for
    topicNames = cgo.keys.map(_.topic).toVector.distinct
    topicInfos <- topicNames
      // folding the topic-info-or-errors by gathering all errors and results as well
      .map(getTopicInfo(_).toValidatedValueOrCommandErrors)
      .sequence[ValidatedValueOrCommandErrors, KafkaTopic.Info].toValueOrCommandErrors
  } yield {
    // starting from the topicInfos so that we list all topics' all partitions, even if they don't have committed offsets
    val consumerGroupTopicInfos = topicInfos.map {
        topicInfo => KafkaConsumerGroup.TopicInfo(
          topicInfo.topic,
          topicInfo.partitions.map {
            case (partition, topicPartitionInfo) =>
              (
                partition,
                KafkaConsumerGroup.TopicPartitionInfo(
                  topicPartitionInfo,
                  // we may or may not have an offset for this
                  cgo.get(new TopicPartition(topicInfo.topic, partition)).map(_.offset)
                )
              )
          }
        )
      }
      .map(ti => (ti.topic, ti))

    KafkaConsumerGroup.Info(
      consumerGroupId,
      cgd.isSimpleConsumerGroup,
      cgd.members().asScala.map(KafkaConsumerGroup.Member.apply).toSeq,
      cgd.partitionAssignor,
      KafkaConsumerGroup.State(cgd.state),
      KafkaNode(cgd.coordinator),
      consumerGroupTopicInfos
    )
  }
}
