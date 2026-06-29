/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.migrate

import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets

private final case class TopologyDeploymentCompact(topologyId: String, status: Json, isDeployed: Boolean)
private final case class TopologyDeploymentStatus(result: Option[String] = None)
private final case class DeploymentsSuccess(statuses: Map[String, TopologyDeploymentStatus])

private final case class HttpResponse(statusCode: Int, text: String) {
  def is2xx: Boolean = statusCode / 100 == 2
}

final case class VnextClient(
  kafkaClusterId: KafkaClusterEntityId,
  urlTemplate: String,
  connectTimeoutMillis: Int,
  readTimeoutMillis: Int
) extends LazyLogging {
  private lazy val url = urlTemplate.replace("{kafkaCluster}", kafkaClusterId.id)
  private lazy val apiUrl = s"$url/api/v1"
  private lazy val deploymentsUrl = s"$apiUrl/deployments"
  private lazy val deploymentsCompactUrl = s"$apiUrl/deployments-compact"

  // Plain java.net.HttpURLConnection so the tool runs on Java 8. (requests-scala >= 0.8.0 uses
  // java.net.http and requires Java 11+, which crashes on this image's Java 8 runtime.)
  private def httpRequest(method: String, urlString: String, body: Option[String], headers: Seq[(String, String)]): HttpResponse = {
    val conn = new URL(urlString).openConnection().asInstanceOf[HttpURLConnection]
    try {
      conn.setRequestMethod(method)
      conn.setConnectTimeout(connectTimeoutMillis)
      conn.setReadTimeout(readTimeoutMillis)
      headers.foreach { case (name, value) => conn.setRequestProperty(name, value) }
      body.foreach { content =>
        conn.setDoOutput(true)
        val out = conn.getOutputStream
        try out.write(content.getBytes(StandardCharsets.UTF_8)) finally out.close()
      }
      val statusCode = conn.getResponseCode
      val stream = if (statusCode >= 200 && statusCode < 400) conn.getInputStream else conn.getErrorStream
      HttpResponse(statusCode, readAll(stream))
    } finally conn.disconnect()
  }

  private def readAll(stream: InputStream): String = {
    if (stream == null) ""
    else {
      try {
        val out = new ByteArrayOutputStream()
        val buffer = new Array[Byte](8192)
        var read = stream.read(buffer)
        while (read != -1) {
          out.write(buffer, 0, read)
          read = stream.read(buffer)
        }
        new String(out.toByteArray, StandardCharsets.UTF_8)
      } finally stream.close()
    }
  }

  def getDeployedTopologyIds: Set[String] = {
    val response = httpRequest("GET", deploymentsCompactUrl, body = None, headers = Nil)
    if (response.is2xx) {
      val topologyDeploymentCompacts = decode[Seq[TopologyDeploymentCompact]](response.text).right.get
      topologyDeploymentCompacts.filter(_.isDeployed).map(_.topologyId).toSet
    } else {
      sys.error(s"GET $deploymentsCompactUrl failed: ${response.statusCode} - ${response.text}")
    }
  }

  def deploy(saveJsonFunc: (KafkaClusterEntityId, Json) => Unit)(deployment: VnextDeployment): Unit = {
    val deploymentJson = deployment.asJson
    saveJsonFunc(kafkaClusterId, deploymentJson)
    logger.info(s"Deploying ${deployment.deploy.size} topologies, deleting ${deployment.delete.size}...")
    val response = httpRequest(
      "POST",
      deploymentsUrl,
      body = Some(deploymentJson.noSpaces),
      headers = Seq("Content-type" -> "application/json")
    )
    if (response.is2xx) {
      val deploymentsSuccess = decode[DeploymentsSuccess](response.text).right.get
      logger.info(s"Deploying succeeded: ${deploymentsSuccess.statuses.collect { case (tid, TopologyDeploymentStatus(Some(result))) => s"$tid: $result" }.mkString(", ") }")
      // TODO check for errors?
    } else {
      sys.error(s"POST $deploymentsUrl failed: ${response.statusCode} - ${response.text}")
    }
  }
}
