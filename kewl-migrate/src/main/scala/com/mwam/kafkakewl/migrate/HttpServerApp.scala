/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.migrate

import akka.Done
import better.files._
import cats.syntax.option._
import com.codahale.metrics.jmx.JmxReporter
import com.mwam.kafkakewl.domain.deploy.{DeployedTopology, DeployedTopologyEntityId}
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.topology.TopologyEntityId
import com.mwam.kafkakewl.utils.{ApplicationMetrics, MdcUtils}
import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import nl.grons.metrics4.scala.DefaultInstrumented

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Promise}

object HttpServerApp extends App
  with LazyLogging
  with HttpServerAppConfigProvider
  with DefaultInstrumented
  with MdcUtils {

  private implicit val executionContext: ExecutionContextExecutor = ExecutionContext.global
  private val jmpReporter: JmxReporter = JmxReporter.forRegistry(metricRegistry).inDomain("kafkakewl-migrate").build

  private var deployedTopologiesByKafkaCluster = Map.empty[KafkaClusterEntityId, Map[DeployedTopologyEntityId, DeployedTopology]]

  jmpReporter.start()
  ApplicationMetrics.initialize()

  logger.info(s"available processors:     ${Runtime.getRuntime.availableProcessors()}")
  logger.info(s"task-support parallelism: ${Seq.fill(50)(1).par.tasksupport.parallelismLevel}")
  logger.info(s"destination path:         ${destinationPath.getOrElse("n/a")}")

  // Clear the destination path (if there is one)
  destinationPath.foreach { path =>
    val dir = file"$path"
    if (dir.exists && dir.isDirectory) {
      logger.info(s"deleting the contents of ${dir}")
      dir.clear()
    } else {
      logger.info(s"${dir} does not exist, creating it")
      dir.createDirectories()
    }
  }

  private def isKafkaClusterRelevant(kafkaClusterId: KafkaClusterEntityId) = vnextInstances.contains(kafkaClusterId)

  private def saveJson(kafkaClusterId: KafkaClusterEntityId, topologyEntityId: TopologyEntityId, result: DeployedTopologyMigrateResult): Unit = {
    destinationPath.foreach { path =>
      val dir = file"$path" / kafkaClusterId.id
      if (!dir.exists) {
        dir.createDirectories()
      }
      val file = dir / s"${topologyEntityId.id}.json"
      result match {
        case DeployedTopologyMigrateResult.Deploy(json) => file.write(json.spaces2)
        case DeployedTopologyMigrateResult.Delete(_) => if (file.exists) file.delete()
      }
    }
  }

  private def saveDeploymentJson(kafkaClusterId: KafkaClusterEntityId, deploymentJson: Json): Unit = {
    destinationPath.foreach { path =>
      val dir = file"$path" / kafkaClusterId.id
      if (!dir.exists) {
        dir.createDirectories()
      }
      val file = dir / "__last_deployment__.json"
      file.write(deploymentJson.spaces2)
    }
  }

  private def mdc(deployedTopology: DeployedTopology): Map[String, String] =
    Map(
      "kafkaCluster" -> deployedTopology.kafkaClusterId.id,
      "topologyId" -> deployedTopology.topologyId.id,
      "deploymentVersion" -> deployedTopology.deploymentVersion.toString
    )

  private def mdc(kafkaClusterId: KafkaClusterEntityId): Map[String, String] =
    Map(
      "kafkaCluster" -> kafkaClusterId.id
    )

  /**
   * Handling the initial set of deployed-topologies after reading the change-log topic until the end.
   *
   * @param deployedTopologies the map of current deployed-topologies
   */
  private def handleInitialDeployedTopologies(deployedTopologies: Map[DeployedTopologyEntityId, DeployedTopology]): Unit = {
    deployedTopologiesByKafkaCluster = deployedTopologies
      .filter { case (_, deployedTopology) => isKafkaClusterRelevant(deployedTopology.kafkaClusterId) }
      .groupBy { case (_, deployedTopology) => deployedTopology.kafkaClusterId }

    logger.info(s"deployed topologies initial snapshot for ${deployedTopologiesByKafkaCluster.map { case (k, t) => s"$k: ${t.size}" }.mkString(", ")}")

    val desiredVnextTopologiesByKafkaCluster = deployedTopologiesByKafkaCluster
      .toSeq
      .sortBy { case (kafkaClusterId, _) => kafkaClusterId.id }
      .map { case (kafkaClusterId, deployedTopologies) =>
        (
          kafkaClusterId,
          deployedTopologies
            .toSeq
            .sortBy { case (id, _) => id.id }
            .map { case (id, deployedTopology) =>
              withMDC(mdc(deployedTopology)) {
                logger.info(s"deployed topology initial snapshot: $id")
                val result = DeployedTopologyMigrate.deploy(deployedTopologiesByKafkaCluster(kafkaClusterId), deployedTopology)
                result.foreach(saveJson(deployedTopology.kafkaClusterId, deployedTopology.topologyId, _))
                (deployedTopology.topologyId.id, result)
              }
            }
            .collect { case (id, Some(DeployedTopologyMigrateResult.Deploy(json))) => (id, json) }
            .toMap
        )
      }
      .toMap

    vnextInstances
      .foreach { case (kafkaClusterId, urlTemplate) =>
        withMDC(mdc(kafkaClusterId)) {
          val vnextClient = VnextClient(kafkaClusterId, urlTemplate)
          val actualVnextTopologIds = vnextClient.getDeployedTopologyIds
          val desiredVnextTopologies = desiredVnextTopologiesByKafkaCluster.getOrElse(kafkaClusterId, Map.empty)
          // We'll deploy all current migrated topologies (they may already be deployed to vnext, but that's fine, it's idempotent)
          val deploy = desiredVnextTopologies.values.toSeq
          // We'll delete all topologies that are in vnext but not in the desired set
          val delete = (actualVnextTopologIds -- desiredVnextTopologies.keys).toSeq
          val vnextDeployment = VnextDeployment(deploy = deploy, delete = delete)
          vnextClient.deploy(saveDeploymentJson)(vnextDeployment)
        }
      }
  }

  /**
   * Handling a new or updated deployed-topology.
   *
   * @param isNew true if it's a new, false if it's an updated deployed-topology
   * @param id the deployed-topology id
   * @param deployedTopology the new/updated deployed-topology itself
   */
  private def handleDeployedTopology(isNew: Boolean, id: DeployedTopologyEntityId, deployedTopology: DeployedTopology): Unit = {
    if (isKafkaClusterRelevant(deployedTopology.kafkaClusterId)) {
      withMDC(mdc(deployedTopology)) {
        if (isNew) {
          logger.info(s"deployed topology created: $id")
        } else {
          logger.info(s"deployed topology updated: $id")
        }
        val kafkaClusterId = deployedTopology.kafkaClusterId
        val deployedTopologies = deployedTopologiesByKafkaCluster.getOrElse(kafkaClusterId, Map.empty) + (id -> deployedTopology)
        deployedTopologiesByKafkaCluster += kafkaClusterId -> deployedTopologies
        val result = DeployedTopologyMigrate.deploy(deployedTopologies, deployedTopology)
        result.foreach(saveJson(deployedTopology.kafkaClusterId, deployedTopology.topologyId, _))
        val vnextDeployment = result match {
          case Some(DeployedTopologyMigrateResult.Deploy(json)) => VnextDeployment(deploy = Seq(json), delete = Seq.empty).some
          case Some(DeployedTopologyMigrateResult.Delete(topologyId)) => VnextDeployment(deploy = Seq.empty, delete = Seq(topologyId)).some
          case None => none
        }
        vnextDeployment.foreach(VnextClient(kafkaClusterId, vnextInstances(kafkaClusterId)).deploy(saveDeploymentJson))
      }
    }
  }

  /**
   * Handling a removed deployed-topology.
   *
   * @param id the deployed-topology id
   * @param deployedTopology the removed deployed-topology itself
   */
  private def handleRemovedDeployedTopology(id: DeployedTopologyEntityId, deployedTopology: DeployedTopology): Unit = {
    if (isKafkaClusterRelevant(deployedTopology.kafkaClusterId)) {
      withMDC(mdc(deployedTopology)) {
        logger.info(s"deployed topology removed: $id")
        val kafkaClusterId = deployedTopology.kafkaClusterId
        val deployedTopologies = deployedTopologiesByKafkaCluster.getOrElse(kafkaClusterId, Map.empty) - id
        deployedTopologiesByKafkaCluster += kafkaClusterId -> deployedTopologies
        val result = DeployedTopologyMigrate.remove(deployedTopologies, deployedTopology)
        result.foreach(saveJson(deployedTopology.kafkaClusterId, deployedTopology.topologyId, _))
        val vnextDeployment = result match {
          case Some(DeployedTopologyMigrateResult.Delete(topologyId)) => VnextDeployment(deploy = Seq.empty, delete = Seq(topologyId)).some
          case None => none
        }
        vnextDeployment.foreach(VnextClient(kafkaClusterId, vnextInstances(kafkaClusterId)).deploy(saveDeploymentJson))
      }
    }
  }

  private val kafkaChangeLogConsumer = new KafkaChangeLogStoreConsumer(
    kafkaChangeLogStoreConnection,
    KafkaChangeLogStoreConsumer.KafkaClusterObserverFunctions(
      kafkaClusters => kafkaClusters.keys.toSeq.sortBy(_.id).foreach(id => logger.info(s"kafka-cluster initial-snapshot: $id")),
      (id, _) => withMDC(mdc(id)) { logger.info(s"kafka-cluster created: $id") } ,
      (id, _, _) => withMDC(mdc(id)) { logger.info(s"kafka-cluster updated: $id") },
      (id, _) => withMDC(mdc(id)) { logger.info(s"kafka-cluster removed: $id") },
    ),
    KafkaChangeLogStoreConsumer.DeployedTopologyObserverFunctions(
      handleInitialDeployedTopologies,
      (id, deployedTopology) => handleDeployedTopology(isNew = true, id, deployedTopology),
      (id, _, deployedTopology) => handleDeployedTopology(isNew = false, id, deployedTopology),
      handleRemovedDeployedTopology
    )
  )

  println(s"Startup completed")

  sys.addShutdownHook {
    logger.info("ShutdownHook beginning...")
    kafkaChangeLogConsumer.stop()
    logger.info("ShutdownHook finished.")
  }

  Await.ready(Promise[Done].future, Duration.Inf)
  logger.info("application exiting.")
}
