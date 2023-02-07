/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster

import com.mwam.kafkakewl.kafka.utils.KafkaExtensions._
import com.mwam.kafkakewl.kafka.utils.{KafkaAdminConfig, KafkaConfigProperties, KafkaConnection}
import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.domain.kafka.KafkaTopic
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaClusterEntityId, NonKewlKafkaResources}
import com.mwam.kafkakewl.processor.kafkacluster.common._
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.KafkaConsumer

class KafkaClusterMetricsHelper(val connection: KafkaConnection, val kafkaClusterId: KafkaClusterEntityId)
  extends KafkaConnectionOperations
    with KafkaConsumerOperations[Array[Byte], Array[Byte]]
    with KafkaAdminClientAndConsumerOperations[Array[Byte], Array[Byte]]
    with KafkaAdminClientOperations
    with KafkaConsumerExtraSyntax
    with KafkaAdminClientExtraSyntax {

  val adminClient: AdminClient = AdminClient.create(KafkaConfigProperties.forAdmin(KafkaAdminConfig(connection)))
  val consumer: KafkaConsumer[Array[Byte], Array[Byte]] = createConsumer(groupId = None)

  val mdc: Map[String, String] = Mdc.fromKafkaClusterId(kafkaClusterId)

  def getAllTopicInfos(nonKewlKafkaResources: Option[NonKewlKafkaResources] = None): ValueOrCommandErrors[(Seq[KafkaTopic.Info], Seq[KafkaTopic.Partition])] =
    for {
      // including internal topics too, primarily because
      // - it may be useful
      // - we may have consumer groups (with offsets) on "__consumer_offsets", it could be useful to monitor their lag
      // - it can't hurt, currently the internal topics are only "__consumer_offsets" and "__transaction_state"
      topicNames <- adminClient.topicNames(includeInternal = true).toRightOrCommandErrors
      topicInfosWithMissingTopicPartitions <- getTopicsInfo(topicNames.filter(t => nonKewlKafkaResources.forall(!_.isTopicNonKewl(t))))
    } yield {
      topicInfosWithMissingTopicPartitions
    }

  def close(): Unit = {
    adminClient.close()
    consumer.close()
  }
}
