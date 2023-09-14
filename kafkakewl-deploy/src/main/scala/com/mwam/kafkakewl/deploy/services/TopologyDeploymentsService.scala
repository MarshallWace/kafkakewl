/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy.services

import com.mwam.kafkakewl.deploy.persistence.PersistentStore
import com.mwam.kafkakewl.domain.*
import zio.*

class TopologyDeploymentsService private (
  private val persistentStore: PersistentStore,
  private val mutex: Semaphore,
  private val topologyDeploymentsRef: Ref[Map[TopologyId, TopologyDeployment]]
) {
  def deploy(deployments: Deployments): Task[DeploymentsResult] =
    mutex.withPermit {
      for {
        _ <- ZIO.logInfo(s"deploying $deployments")
        // TODO authorization
        // TODO validating the deployments
        // TODO performing the kafka deployment itself, returns the new TopologyDeployments
        // TODO persisting the new TopologyDeployments
        // TODO publishing the change-log messages
        // TODO update the in-memory state
        topologyDeployments = deployments.deploy
          .map(t => (t.id, TopologyDeployment(t.id, TopologyDeploymentStatus(), Some(t))))
          .toMap -- deployments.delete
        _ <- persistentStore.save(topologyDeployments)
        _ <- topologyDeploymentsRef.update { _ ++ topologyDeployments -- deployments.delete }
        _ <- ZIO.logInfo(s"finished deploying $deployments")
      }  yield DeploymentsResult()
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
        topologyDeployments <- persistentStore.loadAll().orDie
        mutex <- Semaphore.make(permits = 1)
        topologyDeploymentsRef <- Ref.make(topologyDeployments.map(td => (td.topologyId, td)).toMap)
      } yield TopologyDeploymentsService(persistentStore, mutex, topologyDeploymentsRef)
    }
}
