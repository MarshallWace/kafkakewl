/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.persistence.sql

import java.sql.{CallableStatement, Connection, DriverManager}

import com.mwam.kafkakewl.utils.SqlDbConnectionInfo

trait SqlPersistentStore {
  def getConnection(connectionInfo: SqlDbConnectionInfo): Connection = {
    val connectionString = s"jdbc:sqlserver://${connectionInfo.host}" +
      connectionInfo.port.map(p => s":$p").getOrElse("") + ";" +
      s"databaseName=${connectionInfo.database};" +
      s"authenticationScheme=JavaKerberos;integratedSecurity=true;"

    DriverManager.getConnection(connectionString, null, null)
  }

  def withConnectionAndStatement(
    connectionInfo: SqlDbConnectionInfo,
    createStatement: Connection => CallableStatement
  )(action: (Connection, CallableStatement) => Unit): Unit = {
    val connection = getConnection(connectionInfo)
    try {
      val statement = createStatement(connection)
      try {
        action(connection, statement)
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }
}
