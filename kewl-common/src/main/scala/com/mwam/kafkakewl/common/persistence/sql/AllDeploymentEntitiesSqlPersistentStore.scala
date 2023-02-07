/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.persistence.sql

import com.mwam.kafkakewl.common.AllDeploymentEntities.InMemoryStateStores
import com.mwam.kafkakewl.common.AllDeploymentEntities.InMemoryStateStores._
import com.mwam.kafkakewl.common.AllDeploymentEntities.WritableStateStores._
import com.mwam.kafkakewl.common._
import com.mwam.kafkakewl.common.changelog.ChangeLogStore
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.{AllDeploymentEntitiesStateChanges, EntityStateChangeTransactionItem}
import com.mwam.kafkakewl.utils.SqlDbConnectionInfo
import com.typesafe.scalalogging.Logger
import io.circe.{Decoder, Encoder}
import io.circe.parser._
import io.circe.syntax._

import scala.concurrent.duration.Duration

class AllDeploymentEntitiesSqlPersistentStore(
  env: Env,
  kafkaClusterId: KafkaClusterEntityId,
  connectionInfo: SqlDbConnectionInfo,
  changeLogStore: Option[ChangeLogStore],
  timeoutDuration: Duration,
  logger: Logger,
  startWithWipe: Boolean
)(
  implicit
  encoder: Encoder[EntityStateChangeTransactionItem[AllDeploymentEntitiesStateChanges]],
  decoder: Decoder[EntityStateChangeTransactionItem[AllDeploymentEntitiesStateChanges]]
) extends AllDeploymentEntitiesPersistentStore with SqlPersistentStore {

  if (startWithWipe) wipe()

  def loadLatestVersions(): InMemoryStateStores = {
    val stateStore = AllDeploymentEntities.InMemoryStateStores()

    withConnectionAndStatement(connectionInfo, _.prepareCall(s"{call kewl.DeploymentStateChange_GetAll_Ascending(?)}")) {
      (_, statement) => {
        statement.setString(1, kafkaClusterId.id)
        val resultSet = statement.executeQuery()
        Iterator
          .continually((resultSet.next(), resultSet))
          .takeWhile { case (hasNext, _) => hasNext}
          .map { case (_, rs) => (rs.getString("TransactionId"), rs.getString("DeploymentStateChangeJson"))}
          .foreach { case (_, stateChangeAsJson) =>
            val transactionItem = decode[EntityStateChangeTransactionItem[AllDeploymentEntitiesStateChanges]](stateChangeAsJson).right.get
            stateStore.toWritable.applyEntityStateChange(transactionItem.stateChanges)
          }
      }
    }

    stateStore
  }

  def saveTransactions(transactionItems: IndexedSeq[EntityStateChangeTransactionItem[AllDeploymentEntitiesStateChanges]]): Unit = {
    changeLogStore.foreach(_.saveDeploymentStateChanges(kafkaClusterId, transactionItems))

    withConnectionAndStatement(connectionInfo, _.prepareCall(s"{ call kewl.DeploymentStateChange_Insert( ?, ?, ? ) }")) {
      (_, statement) => {
        transactionItems.foreach { transactionItem =>
          statement.setString(1, kafkaClusterId.id)
          statement.setString(2, transactionItem.transactionId)
          statement.setString(3, transactionItem.asJson.noSpaces)
          statement.execute()
        }
      }
    }
  }

  def wipe(): Unit = {
    withConnectionAndStatement(connectionInfo, _.prepareCall(s"{ call kewl.DeploymentStateChange_Wipe( ? ) }")) {
      (_, statement) => {
        statement.setString(1, kafkaClusterId.id)
        statement.execute()
      }
    }
  }

  def close(): Unit = ()
}
