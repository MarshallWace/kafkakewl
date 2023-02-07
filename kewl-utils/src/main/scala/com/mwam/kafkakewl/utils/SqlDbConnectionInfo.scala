/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.utils

import org.xbill.DNS.{Lookup, SRVRecord, Type}

final case class SqlDbConnectionInfo(host: String, port: Option[Int], database: String) {
  def resolve(suffix: String): SqlDbConnectionInfo = {
    val tcpSuffix = s"_tcp.$suffix"
    val query = s"$host.$tcpSuffix"
    val lookupResponse = Option(new Lookup(query, Type.SRV).run)
      .getOrElse(throw new RuntimeException(s"couldn't lookup '$query'"))

    val response = lookupResponse.map { record =>
      val srv = record.asInstanceOf[SRVRecord]
      val srvHost = srv.getTarget.toString.replaceFirst("\\.$", "")
      val srvPort = srv.getPort
      SqlDbConnectionInfo(srvHost, Some(srvPort), database)
    }

    response.headOption.getOrElse(throw new RuntimeException(s"couldn't find any srv items for '$query'"))
  }
}
