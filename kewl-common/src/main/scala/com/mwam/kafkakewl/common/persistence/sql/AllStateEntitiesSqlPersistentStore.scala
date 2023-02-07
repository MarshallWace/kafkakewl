/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.persistence.sql

import com.mwam.kafkakewl.common.AllStateEntities.InMemoryVersionedStateStores
import com.mwam.kafkakewl.common.AllStateEntities.InMemoryVersionedStateStores._
import com.mwam.kafkakewl.common.AllStateEntities.WritableStateStores._
import com.mwam.kafkakewl.common._
import com.mwam.kafkakewl.common.changelog.ChangeLogStore
import com.mwam.kafkakewl.domain.{AllStateEntitiesStateChanges, EntityStateChangeTransactionItem}
import com.mwam.kafkakewl.utils.SqlDbConnectionInfo
import com.typesafe.scalalogging.Logger
import io.circe.{Decoder, Encoder}
import io.circe.parser._
import io.circe.syntax._

import scala.concurrent.duration.Duration

class AllStateEntitiesSqlPersistentStore(
  env: Env,
  connectionInfo: SqlDbConnectionInfo,
  changeLogStore: Option[ChangeLogStore],
  timeoutDuration: Duration,
  logger: Logger,
  startWithWipe: Boolean
)(
  implicit
  encoder: Encoder[EntityStateChangeTransactionItem[AllStateEntitiesStateChanges]],
  decoder: Decoder[EntityStateChangeTransactionItem[AllStateEntitiesStateChanges]]
) extends AllStateEntitiesPersistentStore with SqlPersistentStore {

  if (startWithWipe) {
    wipe()
    // Wiping the change log too. In theory it's possible that after it's wiped some pending kafka-cluster message processing
    // produces new messages into here, before the kafka-cluster processor is also shut down, but it's unlikely and the admin
    // can re-do the wipe in that case.
    changeLogStore.foreach(_.wipe())
  }

  def loadAllVersions(): InMemoryVersionedStateStores = {
    val stateStore = AllStateEntities.InMemoryVersionedStateStores()

    withConnectionAndStatement(connectionInfo, _.prepareCall(s"{ call kewl.StateChange_GetAll_Ascending }")) {
      (_, statement) => {
        val resultSet = statement.executeQuery()
        Iterator
          .continually((resultSet.next(), resultSet))
          .takeWhile { case (hasNext, _) => hasNext}
          .map { case (_, rs) => (rs.getString("TransactionId"), rs.getString("StateChangeJson"))}
          .foreach { case (_, stateChangeAsJson) =>
            val transactionItem = decode[EntityStateChangeTransactionItem[AllStateEntitiesStateChanges]](stateChangeAsJson).right.get
            stateStore.toWritable.applyEntityStateChange(transactionItem.stateChanges)
          }
      }
    }

    stateStore
  }

  def saveTransactions(transactionItems: IndexedSeq[EntityStateChangeTransactionItem[AllStateEntitiesStateChanges]]): Unit = {
    changeLogStore.foreach(_.saveStateChanges(transactionItems))

    withConnectionAndStatement(connectionInfo, _.prepareCall(s"{ call kewl.StateChange_Insert( ?, ? ) }")) {
      (_, statement) => {
        transactionItems.foreach { transactionItem =>
          statement.setString(1, transactionItem.transactionId)
          statement.setString(2, transactionItem.asJson.noSpaces)
          statement.execute()
        }
      }
    }
  }

  def wipe(): Unit = {
    withConnectionAndStatement(connectionInfo, _.prepareCall(s"{ call kewl.StateChange_Wipe }")) {
      (_, statement) => {
        statement.execute()
      }
    }
  }

  def close(): Unit = ()
}
