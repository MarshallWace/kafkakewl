/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package deploy

import com.mwam.kafkakewl.domain.kafkacluster.{KafkaClusterAndTopology, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.topology.TopologyEntityId

final case class DeploymentEntityId(id: String) extends AnyVal with EntityId

/**
  * The possible topology versions to deploy.
  */
sealed trait DeploymentTopologyVersion
object DeploymentTopologyVersion {
  /**
    * Indicates that this topology shouldn't be deployed to this cluster (it should be removed).
    *
    * A deployment can be deleted only once this is set, and the deployment-status successfully reflects the un-deploy.
    */
  final case class Remove() extends DeploymentTopologyVersion

  /**
    * It deploys that exact version of the topology.
    *
    * @param exact the version of the topology to be deployed.
    */
  final case class Exact(exact: Int) extends DeploymentTopologyVersion

  /**
    * It means it'll automatically deploy the latest topologies.
    */
  final case class LatestTracking() extends DeploymentTopologyVersion
}

/**
  * Entity type that describes a deployment: what topology to what cluster should be deployed in what way.
  *
  * This defines the desired state of the deployment. The actual deployment is described in the DeploymentStatus entity.
  *
  * @param topologyId the topology id to be deployed
  * @param kafkaClusterId the kafka cluster where it should be deployed
  * @param topologyVersion the version of the topology to be deployed
  * @param tags the optional list of tags
  * @param labels the optional labels
  */
final case class Deployment(
  kafkaClusterId: KafkaClusterEntityId,
  topologyId: TopologyEntityId,
  topologyVersion: DeploymentTopologyVersion,
  tags: Seq[String] = Seq.empty,
  labels: Map[String, String] = Map.empty
) extends Entity with Labelled

object Deployment {
  def parseId(id: DeploymentEntityId): (KafkaClusterEntityId, TopologyEntityId) = KafkaClusterAndTopology.parseId(id.id)
}

object DeploymentStateChange {
  sealed trait StateChange extends SimpleEntityStateChange[Deployment]
  final case class NewVersion(metadata: EntityStateMetadata, entity: Deployment) extends StateChange with SimpleEntityStateChange.NewVersion[Deployment]
  final case class Deleted(metadata: EntityStateMetadata) extends StateChange with SimpleEntityStateChange.Deleted[Deployment]
}
