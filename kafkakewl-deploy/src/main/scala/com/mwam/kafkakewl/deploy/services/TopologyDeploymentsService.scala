/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy.services

import com.mwam.kafkakewl.common.persistence.PersistentStore
import com.mwam.kafkakewl.domain.*
import com.mwam.kafkakewl.domain.validation.TopologyValidation
import zio.*

class TopologyDeploymentsService private (
  private val persistentStore: PersistentStore,
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
        _ <- TopologyValidation.validate(topologyDeploymentsBefore)(deployments)
          .toZIOParallelErrors
          .mapError(DeploymentsFailure.validation)

        // TODO performing the kafka deployment itself, returns the new TopologyDeployments
        // TODO persisting the new TopologyDeployments
        // TODO publishing the change-log messages
        // TODO update the in-memory state

        // Just same fake topology deployments for now
        topologyDeployments = deployments.deploy
          .map(t => (t.id, TopologyDeployment(t.id, TopologyDeploymentStatus(), Some(t))))
          .toMap ++ deployments.delete
            .map(tid => (tid, TopologyDeployment(tid, TopologyDeploymentStatus(), None)))
            .toMap

        _ <- persistentStore.save(topologyDeployments).logError("saving TopologyDeployments").mapError(DeploymentsFailure.persistence)
        _ <- topologyDeploymentsRef.update { _ ++ topologyDeployments -- deployments.delete }
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
  def live: ZLayer[PersistentStore, Nothing, TopologyDeploymentsService] =
    ZLayer.fromZIO {
      for {
        persistentStore <- ZIO.service[PersistentStore]
        // TODO what happens if we fail here?
        topologyDeployments <- persistentStore.loadLatest().orDie
        mutex <- Semaphore.make(permits = 1)
        topologyDeploymentsRef <- Ref.make(topologyDeployments)
      } yield TopologyDeploymentsService(persistentStore, mutex, topologyDeploymentsRef)
    }
}
