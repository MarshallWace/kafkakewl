/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import com.mwam.kafkakewl.domain.{AllDeploymentEntitiesStateChanges, EntityStateChangeTransactionItem}

trait AllDeploymentEntitiesPersistentStore {
  def loadLatestVersions(): AllDeploymentEntities.InMemoryStateStores
  def saveTransactions(transactionItems: IndexedSeq[EntityStateChangeTransactionItem[AllDeploymentEntitiesStateChanges]]): Unit
  def wipe(): Unit
  def close(): Unit
}
