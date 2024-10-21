/*
 * SPDX-FileCopyrightText: 2024 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import arrow.core.NonEmptyList
import kotlinx.serialization.Serializable

@Serializable
data class DeploymentOptions(
    val dryRun: Boolean = true,
    // TODO make allowing unsafe operations more granular if needed
    val allowUnsafe: Boolean = false
)

/** A deployment to be done, contains options, topologies to be deployed and topology-ids to be removed.
 *
 * @param options
 *   the deployment options
 * @param deploy
 *   the topologies to deploy
 * @param delete
 *   the topology-ids to remove
 */
@Serializable
data class Deployments(
    val options: DeploymentOptions = DeploymentOptions(),
    val deploy: List<Topology> = emptyList(),
    val delete: List<TopologyId> = emptyList()
)

@Serializable
data class TopologyDeploymentQuery(
    val topologyIdFilterRegex: String?,
    val offset: Int?,
    val limit: Int?
)

@Serializable
data class TopologyDeploymentStatus(
    val result: String = "" // TODO we may need to replace it in the future with something more appropriate
)

/** The deployment of a topology
 *
 * @param topologyId
 *   the topology id
 * @param status
 *   the deployment status
 * @param topology
 *   the optional topology, null means the topology is removed
 */
@Serializable
data class TopologyDeployment(
    val topologyId: TopologyId,
    val status: TopologyDeploymentStatus,
    val topology: Topology?
) {
    fun toCompact(): TopologyDeploymentCompact = TopologyDeploymentCompact(topologyId, status, topology != null)
}

typealias TopologyDeployments = Map<TopologyId, TopologyDeployment>

/** The compact deployment of a topology (it does not contain the topology)
 *
 * @param topologyId
 *   the topology id
 * @param status
 *   the deployment status
 * @param isDeployed
 *   true if the topology is deployed, false if it's removed
 */
@Serializable
data class TopologyDeploymentCompact(
    val topologyId: TopologyId,
    val status: TopologyDeploymentStatus,
    val isDeployed: Boolean
)

typealias TopologyDeploymentsCompact = Map<TopologyId, TopologyDeploymentCompact>

/** Base interface for failures while querying deployments. */
sealed interface QueryDeploymentsFailure

/** Base interface for failures while performing deployments. */
sealed interface PostDeploymentsFailure

/** All failures relating to performing/querying deployments.
 */
object DeploymentsFailure {
    @Serializable
    data class Validation(val validationFailed: List<String>) : PostDeploymentsFailure
    @Serializable
    data class Deployment(val deploymentFailed: List<String>) : PostDeploymentsFailure

    fun validation(errors: NonEmptyList<String>): Validation = Validation(errors)
    fun deployment(throwable: Throwable): Deployment = Deployment(errors(throwable))

}

/** All generic failures
 */
object Failure {
    @Serializable
    data class NotFound(val notFound: List<String>) : QueryDeploymentsFailure
    @Serializable
    data class Authorization(val authorizationFailed: List<String>) : PostDeploymentsFailure, QueryDeploymentsFailure

    fun notFound(vararg notFound: String): NotFound = NotFound(notFound.toList())
    fun authorization(throwable: Throwable): Authorization = Authorization(errors(throwable))
}

private fun errors(throwable: Throwable): List<String> = listOf(throwable.message ?: "???")

/** The result of a successful deployment.
 *
 * @param statuses
 *   the statuses of the topology deployments.
 */
@Serializable
data class DeploymentsSuccess(val statuses: Map<TopologyId, TopologyDeploymentStatus>) : QueryDeploymentsFailure
