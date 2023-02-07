/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.changelog

import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.{AllDeploymentEntitiesStateChanges, AllStateEntitiesStateChanges, EntityStateChangeTransactionItem}

/**
  * A trait for change-log stores. A change-log store can save state/deployment transactions into a single sequential change-log.
  */
trait ChangeLogStore {
  def saveStateChanges(transactionItems: IndexedSeq[EntityStateChangeTransactionItem[AllStateEntitiesStateChanges]]): Unit
  def saveDeploymentStateChanges(kafkaClusterId: KafkaClusterEntityId, transactionItems: IndexedSeq[EntityStateChangeTransactionItem[AllDeploymentEntitiesStateChanges]]): Unit
  def wipe(): Unit
  def close(): Unit
}
