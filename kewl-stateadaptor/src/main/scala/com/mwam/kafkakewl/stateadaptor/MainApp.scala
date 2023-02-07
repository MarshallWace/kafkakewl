/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.stateadaptor

import akka.Done
import com.mwam.kafkakewl.stateadaptor.destinations.{LatestDeployedTopologyStateAdaptor, LatestGlobalStateAdaptor}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Promise}
import scala.concurrent.duration._

object MainApp extends App
  with LazyLogging with MainAppConfigProvider {

  implicit val executionContext: ExecutionContextExecutor = ExecutionContext.global

  val timeout: Duration = 5.minutes

  logger.info(s"change-log:  $kafkaChangeLogStoreConnection")
  logger.info(s"destinations:")
  destinationLatestDeployedTopologyStateClusterConfigs.foreach { latestDeployedTopologyClusterConfig =>
    logger.info(s"destination: latest-deployedtopology (${latestDeployedTopologyClusterConfig.kafkaClusterId} - ${latestDeployedTopologyClusterConfig.destinationTopicName} - ${latestDeployedTopologyClusterConfig.destinationKafkaConnection})")
  }
  logger.info(s"destination: latest-global ($destinationLatestGlobalStateTopic - $destinationLatestGlobalStateKafkaCluster)")
  logger.info(s"creating and starting adaptors...")

  val latestDeployedTopologyStateAdaptors = destinationLatestDeployedTopologyStateClusterConfigs
    .map(new LatestDeployedTopologyStateAdaptor(kafkaChangeLogStoreConnection, _, timeout))

  val latestGlobalStateAdaptors = Seq(
    new LatestGlobalStateAdaptor(
      kafkaChangeLogStoreConnection,
      destinationLatestGlobalStateConsumerGroup,
      LatestGlobalStateAdaptor.Config(destinationLatestGlobalStateKafkaCluster, destinationLatestGlobalStateTopic),
      timeout
    )
  )

  val adaptors = latestDeployedTopologyStateAdaptors ++ latestGlobalStateAdaptors

  println(s"Startup completed, adaptors are running...")

  val finished = for {
    waitOnFuture  <- Promise[Done].future
  } yield waitOnFuture

  sys.addShutdownHook {
    logger.info("ShutdownHook beginning...")
    adaptors.foreach(_.stop())
    logger.info("ShutdownHook finished.")
  }

  Await.ready(finished, Duration.Inf)
  logger.info("application exiting.")
}
