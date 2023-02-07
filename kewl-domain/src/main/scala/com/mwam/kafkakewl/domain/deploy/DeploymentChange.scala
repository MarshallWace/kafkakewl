/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package deploy

import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.topology.{FlexibleTopologyTopicId, TopologyEntityId}

/**
  * The possible topology versions to deploy. It has LatestOnce as well which is missing from DeploymentTopologyVersion,
  * because LatestOnce is resolved and converted to Exact.
  */
sealed trait DeploymentChangeTopologyVersion
object DeploymentChangeTopologyVersion {
  /**
    * Indicates that this topology shouldn't be deployed to this cluster (it should be removed).
    *
    * A deployment can be deleted only once this is set, and the deployment-status successfully reflects the un-deploy.
    */
  final case class Remove() extends DeploymentChangeTopologyVersion

  /**
    * It deploys that exact version of the topology.
    *
    * @param exact the version of the topology to be deployed.
    */
  final case class Exact(exact: Int) extends DeploymentChangeTopologyVersion

  /**
    * It deploys the latest topology, but doesn't do anything when there is a new version of the topology.
    */
  final case class LatestOnce() extends DeploymentChangeTopologyVersion

  /**
    * It means it'll automatically deploy the latest topologies.
    */
  final case class LatestTracking() extends DeploymentChangeTopologyVersion
}

/**
  * Describes an allowed but unsafe kafka cluster change
  *
  * @param operation the allowed operation or None if every operation is allowed
  * @param entityType the allowed entity type or None if every entity type is allowed
  * @param entityKey the allowed entity key or None if every entity key is allowed
  * @param entityPropertyName the allowed entity property name or None if every property name is allowed
  */
final case class DeploymentAllowUnsafeKafkaClusterChange(
  operation: Option[UnsafeKafkaClusterChangeOperation] = None,
  entityType: Option[KafkaClusterItemEntityType] = None,
  entityKey: Option[String] = None,
  entityPropertyName: Option[String] = None
)

/**
  * Various options for deployment-operations.
  *
  * @param topicsToRecreate the topics to be deleted and created again.
  * @param allowedUnsafeChanges the set of allowed changes that are unsafe to execute.
  * @param topicsToRecreateResetGroups true if the non-live consumer groups must be reset for the topics that are deleted for re-creation.
  * @param authorizationCode an optional authorization code that allows critical deployments.
  */
final case class DeploymentOptions(
  topicsToRecreate: Seq[FlexibleTopologyTopicId] = Seq.empty,
  topicsToRecreateResetGroups: Boolean = true,
  allowedUnsafeChanges: Seq[DeploymentAllowUnsafeKafkaClusterChange] = Seq.empty,
  authorizationCode: Option[String] = None
)

/**
  * A change for a deployment (either a new deployment or an update): what topology to what cluster should be deployed in what way.
  *
  * This defines the desired state of the deployment. It's slightly different from the Deployment entity which contains a resolved LatestOnce
  * topology version, and doesn't contain the allowed unsafe changes.
  *
  * @param topologyId the topology id to be deployed (the default value is there only so that users don't need to specify it when it's coming the url)
  * @param kafkaClusterId the kafka cluster where it should be deployed (the default value is there only so that users don't need to specify it when it's coming the url)
  * @param topologyVersion the version of the topology to be deployed
  * @param options the deployment options
  * @param tags the optional list of tags
  * @param labels the optional labels
  */
final case class DeploymentChange(
  kafkaClusterId: KafkaClusterEntityId = KafkaClusterEntityId(""),
  topologyId: TopologyEntityId = TopologyEntityId(""),
  topologyVersion: DeploymentChangeTopologyVersion = DeploymentChangeTopologyVersion.LatestOnce(),
  options: DeploymentOptions = DeploymentOptions(),
  tags: Seq[String] = Seq.empty,
  labels: Map[String, String] = Map.empty
) extends Labelled
