/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

final case class DeploymentOptions(
  // TODO make allowing unsafe operations more granular if needed
  allowUnsafe: Boolean
)

/**
 * A deployment to be done, contains options, topologies to be deployed and topology-ids to be removed.
 *
 * @param options the deployment options
 * @param deploy the topologies to deploy
 * @param delete the topology-ids to remove
 */
final case class Deployments(
  options: DeploymentOptions,
  deploy: Seq[Topology],
  delete: Seq[TopologyId]
)

final case class DeploymentsResult()

final case class TopologyDeploymentQuery(
  topologyIdFilterRegex: Option[String],
  withTopology: Option[Boolean],
  offset: Option[Int],
  limit: Option[Int]
)

final case class TopologyDeploymentStatus(
  // TODO
)

/**
 * The deployment of a topology
 *
 * @param topologyId the topology id
 * @param status the deployment status
 * @param topology the optional topology, none means the topology is removed
 */
final case class TopologyDeployment(
  topologyId: TopologyId,
  status: TopologyDeploymentStatus,
  topology: Option[Topology]
)
