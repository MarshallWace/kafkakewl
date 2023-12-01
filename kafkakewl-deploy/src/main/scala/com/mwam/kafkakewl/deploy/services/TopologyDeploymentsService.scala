/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy.services

import com.mwam.kafkakewl.common.persistence.PersistentStore
import com.mwam.kafkakewl.domain.*
import com.mwam.kafkakewl.domain.validation.*
import zio.*

class TopologyDeploymentsService private (
    private val persistentStore: PersistentStore,
    private val topologyDeploymentsToKafkaService: TopologyDeploymentsToKafkaService,
    private val mutex: Semaphore,
    private val topologyDeploymentsRef: Ref[TopologyDeployments]
) {
  def deploy(deployments: Deployments): IO[PostDeploymentsFailure, DeploymentsSuccess] =
    mutex.withPermit {
      for {
        _ <- ZIO.logInfo(s"deploying $deployments")

        // TODO authorization

        // Validation before deployment
        topologyDeploymentsBefore <- topologyDeploymentsRef.get
        _ <- DeploymentsValidation
          .validate(topologyDeploymentsBefore.toTopologies, deployments)
          .toZIOParallelErrors
          .mapError(DeploymentsFailure.validation)

        // Performing the actual deployment to kafka (dryRun is respected here)
        success <- topologyDeploymentsToKafkaService
          .deploy(deployments)
          .logError("deploying TopologyDeployments")
          .mapError(DeploymentsFailure.deployment)

        // Creating the new TopologyDeployments from the statuses
        topologyDeployments = deployments.deploy
          .map(t => (t.id, TopologyDeployment(t.id, success.statuses(t.id), Some(t))))
          .toMap ++ deployments.delete
          .map(tid => (tid, TopologyDeployment(tid, success.statuses(tid), None)))
          .toMap

        _ <- (for {
          // Persisting the new TopologyDeployments
          _ <- persistentStore.save(topologyDeployments).logError("saving TopologyDeployments").mapError(DeploymentsFailure.persistence)

          // Updating the in-memory state if everything succeeded so far
          _ <- topologyDeploymentsRef.update {
            _ ++ topologyDeployments -- deployments.delete
          }
        } yield ()).unless(deployments.options.dryRun) // Not making any actual changes if dryRun = true

        _ <- ZIO.logInfo(s"finished deploying $deployments")
      } yield DeploymentsSuccess(
        topologyDeployments
          .map((tid, td) => (tid, td.status))
          .toMap
      )
    }

  def getTopologyDeployments(topologyDeploymentQuery: TopologyDeploymentQuery): UIO[Seq[TopologyDeployment]] = for {
    // TODO use the query
    topologyDeployments <- topologyDeploymentsRef.get
  } yield topologyDeployments.values.toList

  def getTopologyDeployment(id: TopologyId): UIO[Option[TopologyDeployment]] = for {
    topologies <- topologyDeploymentsRef.get
  } yield topologies.get(id)
}

object TopologyDeploymentsService {
  def live: ZLayer[PersistentStore & TopologyDeploymentsToKafkaService, Nothing, TopologyDeploymentsService] =
    ZLayer.fromZIO {
      for {
        persistentStore <- ZIO.service[PersistentStore]
        topologyDeploymentsToKafkaService <- ZIO.service[TopologyDeploymentsToKafkaService]
        // TODO what happens if we fail here?
        topologyDeployments <- persistentStore.loadLatest().orDie
        mutex <- Semaphore.make(permits = 1)
        topologyDeploymentsRef <- Ref.make(topologyDeployments)
      } yield TopologyDeploymentsService(persistentStore, topologyDeploymentsToKafkaService, mutex, topologyDeploymentsRef)
    }
}
