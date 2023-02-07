/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.common

import com.mwam.kafkakewl.domain.{CommandError, _}
import com.mwam.kafkakewl.kafka
import com.mwam.kafkakewl.kafka.utils.KafkaExtensions._
import org.apache.kafka.clients.admin.AdminClient

trait KafkaAdminClientOperations {
  val adminClient: AdminClient

  def ensureConsumerGroupIsNotLive(consumerGroupId: String): ValueOrCommandErrors[Unit] = for {
    cgd <- adminClient.describeConsumerGroup(consumerGroupId).toRightOrCommandErrors
    _ <- kafka.utils.ensureConsumerGroupDescriptionIsNotLive(cgd).left.map(e => Seq(CommandError.otherError(e)))
  } yield ()
}

trait KafkaAdminClientExtraSyntax {
  implicit class KafkaAdminClientExtraSyntaxExtensions(val adminClient: AdminClient) extends KafkaAdminClientOperations
}