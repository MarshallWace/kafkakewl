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

private final case class TopologyDeploymentCompact(topologyId: String, status: Json, isDeployed: Boolean)
private final case class TopologyDeploymentStatus(result: Option[String] = None)
private final case class DeploymentsSuccess(statuses: Map[String, TopologyDeploymentStatus])

final case class VnextClient(kafkaClusterId: KafkaClusterEntityId, urlTemplate: String) extends LazyLogging {
  private lazy val url = urlTemplate.replace("{kafkaCluster}", kafkaClusterId.id)
  private lazy val apiUrl = s"$url/api/v1"
  private lazy val deploymentsUrl = s"$apiUrl/deployments"
  private lazy val deploymentsCompactUrl = s"$apiUrl/deployments-compact"

  def getDeployedTopologyIds: Set[String] = {
    val response = requests.get(deploymentsCompactUrl)
    val responseText = response.text()
    if (response.is2xx) {
      val topologyDeploymentCompacts = decode[Seq[TopologyDeploymentCompact]](responseText).right.get
      topologyDeploymentCompacts.filter(_.isDeployed).map(_.topologyId).toSet
    } else {
      sys.error(s"GET $deploymentsCompactUrl failed: ${response.statusCode} - $responseText")
    }
  }

  def deploy(saveJsonFunc: (KafkaClusterEntityId, Json) => Unit)(deployment: VnextDeployment): Unit = {
    val deploymentJson = deployment.asJson
    saveJsonFunc(kafkaClusterId, deploymentJson)
    logger.info(s"Deploying ${deployment.deploy.size} topologies, deleting ${deployment.delete.size}...")
    val response = requests.post(deploymentsUrl, data = deploymentJson.noSpaces, headers = ("Content-type", "application/json") :: Nil)
    val responseText = response.text()
    if (response.is2xx) {
      val deploymentsSuccess = decode[DeploymentsSuccess](responseText).right.get
      logger.info(s"Deploying succeeded: ${deploymentsSuccess.statuses.collect { case (tid, TopologyDeploymentStatus(Some(result))) => s"$tid: $result" }.mkString(", ") }")
      // TODO check for errors?
    } else {
      sys.error(s"POST $deploymentsUrl failed: ${response.statusCode} - $responseText")
    }
  }
}
