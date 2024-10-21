/*
 * SPDX-FileCopyrightText: 2024 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy.services

import arrow.core.Either
import arrow.core.right
import com.mwam.kafkakewl.common.persistence.PersistentStore
import com.mwam.kafkakewl.domain.Deployments
import com.mwam.kafkakewl.domain.DeploymentsSuccess
import com.mwam.kafkakewl.domain.PostDeploymentsFailure
import com.mwam.kafkakewl.domain.TopologyDeployment
import com.mwam.kafkakewl.domain.TopologyDeploymentQuery
import com.mwam.kafkakewl.domain.TopologyDeploymentStatus
import com.mwam.kafkakewl.domain.TopologyDeployments
import com.mwam.kafkakewl.domain.TopologyId
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.atomic.AtomicReference

interface TopologyDeploymentsService {
    suspend fun deploy(deployments: Deployments): Either<PostDeploymentsFailure, DeploymentsSuccess>
    fun getTopologyDeployments(topologyDeploymentQuery: TopologyDeploymentQuery): List<TopologyDeployment>
    fun getTopologyDeployment(id: TopologyId): TopologyDeployment?
}

private val logger = KotlinLogging.logger {}

class TopologyDeploymentsServiceImpl private constructor(
    private val persistentStore: PersistentStore,
    private val topologyDeploymentsRef: AtomicReference<TopologyDeployments>
) : TopologyDeploymentsService {
    companion object {
        fun create(persistentStore: PersistentStore): TopologyDeploymentsServiceImpl {
            // TODO is there a better way? Maybe loadLatest() could just throw instead of returning a Result<TopologyDeployment>?
            val topologyDeployments = persistentStore.loadLatest().getOrThrow()
            logger.info { "Loaded ${topologyDeployments.size} topology deployments" }
            return TopologyDeploymentsServiceImpl(persistentStore, AtomicReference(topologyDeployments))
        }
    }

    private val mutex = Mutex()

    override suspend fun deploy(deployments: Deployments): Either<PostDeploymentsFailure, DeploymentsSuccess> {
        logger.info { "Trying to enter the deploy-mutex..." }
        mutex.withLock {
            val deploymentsLog = "${deployments.deploy.size}, deleting ${deployments.delete.size} topologies with options = ${deployments.options} (deploying ${deployments.deploy.map { it.id }}, deleting ${deployments.delete})"
            logger.info { "Deploying $deploymentsLog..." }

            // TODO authorization
            // TODO Validation before deployment
            // TODO Performing the actual deployment to kafka (dryRun is respected here)
            // For now we just fake successful deployments and deletes
            val success = DeploymentsSuccess(
                (deployments.deploy.map { it.id } + deployments.delete)
                    .associate { it to TopologyDeploymentStatus() }
            )
            // TODO Creating the new TopologyDeployments from the statuses
            val topologyDeploymentsDeployed = deployments.deploy.associate { it.id to TopologyDeployment(it.id, success.statuses[it.id]!!, it) }
            val topologyDeploymentsDeleted = deployments.delete.associate { it to TopologyDeployment(it, success.statuses[it]!!, null) }
            val topologyDeploymentsDeployedDeleted = topologyDeploymentsDeployed + topologyDeploymentsDeleted

            if (!deployments.options.dryRun) {
                // Persisting the new TopologyDeployments
                persistentStore.save(topologyDeploymentsDeployedDeleted)
                // Updating the in-memory state if everything succeeded so far
                topologyDeploymentsRef.set(topologyDeploymentsRef.get() + topologyDeploymentsDeployed - topologyDeploymentsDeleted.keys)
            }

            logger.info { "Finished deploying $deploymentsLog" }

            return success.right()
        }
    }

    override fun getTopologyDeployments(query: TopologyDeploymentQuery): List<TopologyDeployment> {
        // getting the filtered, ordered list of topology deployments
        val filterRegexOrNull = query.topologyIdFilterRegex?.toRegex()
        val topologyDeployments = topologyDeploymentsRef.get().values
            .filter { filterRegexOrNull == null || filterRegexOrNull.matches(it.topologyId.value) }
            .sortedBy { it.topologyId.value }

        // selecting a range based on offset/limit
        val fromIndex = query.offset ?: 0
        val toIndex = (query.limit?.let { it + fromIndex } ?: topologyDeployments.size).coerceAtMost(topologyDeployments.size)
        return topologyDeployments
            .subList(fromIndex, toIndex)
            .toList()
    }

    override fun getTopologyDeployment(id: TopologyId): TopologyDeployment? = topologyDeploymentsRef.get()[id]
}
