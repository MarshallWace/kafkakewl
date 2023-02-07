/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import com.mwam.kafkakewl.domain.{AllStateEntitiesStateChanges, EntityStateChangeTransactionItem}

trait AllStateEntitiesPersistentStore {
  def loadAllVersions(): AllStateEntities.InMemoryVersionedStateStores
  def saveTransactions(transactionItems: IndexedSeq[EntityStateChangeTransactionItem[AllStateEntitiesStateChanges]]): Unit
  def wipe(): Unit
  def close(): Unit
}
