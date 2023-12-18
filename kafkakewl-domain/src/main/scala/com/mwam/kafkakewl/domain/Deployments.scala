/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import zio.NonEmptyChunk

final case class DeploymentOptions(
    dryRun: Boolean = true,
    // TODO make allowing unsafe operations more granular if needed
    allowUnsafe: Boolean = false
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
final case class Deployments(
    options: DeploymentOptions = DeploymentOptions(),
    deploy: Seq[Topology] = Seq.empty,
    delete: Seq[TopologyId] = Seq.empty
)

final case class TopologyDeploymentQuery(
    topologyIdFilterRegex: Option[String],
    withTopology: Option[Boolean],
    offset: Option[Int],
    limit: Option[Int]
)

final case class TopologyDeploymentStatus(
    // TODO
)

/** The deployment of a topology
  *
  * @param topologyId
  *   the topology id
  * @param status
  *   the deployment status
  * @param topology
  *   the optional topology, none means the topology is removed
  */
final case class TopologyDeployment(
    topologyId: TopologyId,
    status: TopologyDeploymentStatus,
    topology: Option[Topology]
)

type TopologyDeployments = Map[TopologyId, TopologyDeployment]

extension (topologyDeployments: TopologyDeployments) {
  def toTopologies: Topologies = topologyDeployments.values.collect { case TopologyDeployment(tid, _, Some(topology)) => (tid, topology) }.toMap
}

/** Base trait for failures while querying deployments.
  */
sealed trait QueryDeploymentsFailure

/** Base trait for failures while performing deployments.
  */
sealed trait PostDeploymentsFailure

/** All failures relating to performing/querying deployments.
  */
object DeploymentsFailure {
  final case class NotFound(notFound: Seq[String]) extends QueryDeploymentsFailure
  final case class Authorization(authorizationFailed: Seq[String]) extends PostDeploymentsFailure with QueryDeploymentsFailure
  final case class Validation(validationFailed: Seq[String]) extends PostDeploymentsFailure
  final case class Deployment(deploymentFailed: Seq[String]) extends PostDeploymentsFailure
  final case class Persistence(persistFailed: Seq[String]) extends PostDeploymentsFailure
  final case class Timeout(timeout: String) extends PostDeploymentsFailure with QueryDeploymentsFailure

  def notFound(notFound: String*): NotFound = NotFound(notFound)
  def authorization(throwable: Throwable): Authorization = Authorization(errors(throwable))
  def validation(errors: NonEmptyChunk[String]): Validation = Validation(errors)
  def deployment(throwable: Throwable): Deployment = Deployment(errors(throwable))
  def persistence(throwable: Throwable): Persistence = Persistence(errors(throwable))
  def timeout: Timeout = Timeout("Request timed out. Please try again later.")

  private def errors(throwable: Throwable): Seq[String] = Seq(throwable.getMessage)
}

/** The result of a successful deployment.
  *
  * @param statuses
  *   the statuses of the topology deployments.
  */
final case class DeploymentsSuccess(statuses: Map[TopologyId, TopologyDeploymentStatus])
