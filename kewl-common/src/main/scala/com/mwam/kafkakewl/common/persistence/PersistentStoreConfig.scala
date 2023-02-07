/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

package persistence

import com.mwam.kafkakewl.kafka.utils.KafkaConnection
import com.mwam.kafkakewl.utils.SqlDbConnectionInfo

sealed trait PersistentStoreConfig
object PersistentStoreConfig {
  final case class Kafka(
    connection: KafkaConnection,
    stateEntitiesChangeTopicConfig: KafkaKewlSystemTopicConfig,
    deploymentEntitiesChangeTopicConfig: KafkaKewlSystemTopicConfig,
    kafkaTransactionalIdForStateEntities: String,
    kafkaTransactionalIdForDeploymentEntities: String
  ) extends PersistentStoreConfig
  final case class Sql(connectionInfo: SqlDbConnectionInfo) extends PersistentStoreConfig
}
