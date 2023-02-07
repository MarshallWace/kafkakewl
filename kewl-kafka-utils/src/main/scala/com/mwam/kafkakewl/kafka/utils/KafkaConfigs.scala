/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.kafka.utils

final case class KafkaProducerConfig(
  connection: KafkaConnection,
  idempotent: Option[Boolean] = None,
  transactionalId: Option[String] = None
) {
  def makeIdempotent(): KafkaProducerConfig = copy(idempotent = Some(true))
  def makeTransactional(transactionalId: String): KafkaProducerConfig = copy(transactionalId = Some(transactionalId))
}

final case class KafkaConsumerConfig(
  connection: KafkaConnection,
  groupId: Option[String],
  isolationLevel: Option[String] = Some("read_committed"),
  enableAutoCommit: Boolean = false,
  autoOffsetReset: Option[String] = Some("earliest"),
  kafkaRequestTimeOutMillis: Option[Int] = None
) {
  def withGroupId(groupId: String) : KafkaConsumerConfig = copy(groupId = Some(groupId))
  def enableAutoCommit(enableAutoCommit: Boolean) : KafkaConsumerConfig = copy(enableAutoCommit = enableAutoCommit)
  def autoOffsetReset(autoOffsetReset: String) : KafkaConsumerConfig = copy(autoOffsetReset = Some(autoOffsetReset))
}

final case class KafkaAdminConfig(connection: KafkaConnection, kafkaRequestTimeOutMillis: Option[Int] = None)
