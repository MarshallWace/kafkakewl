/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import com.mwam.kafkakewl.domain.deploy.{DeployedTopology, DeployedTopologyStateChange, Deployment, DeploymentStateChange}
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterStateChange}
import com.mwam.kafkakewl.domain.permission.{Permission, PermissionStateChange}
import com.mwam.kafkakewl.domain.topology.{Topology, TopologyStateChange}

object ApplyStateChanges {
  implicit val permissionApplyStateChange: ApplyStateChange[Permission, PermissionStateChange.StateChange] = ApplyStateChange.simple
  implicit val topologyApplyStateChange: ApplyStateChange[Topology, TopologyStateChange.StateChange] = ApplyStateChange.simple
  implicit val kafkaClusterApplyStateChange: ApplyStateChange[KafkaCluster, KafkaClusterStateChange.StateChange] = ApplyStateChange.simple
  implicit val deploymentApplyStateChange: ApplyStateChange[Deployment, DeploymentStateChange.StateChange] = ApplyStateChange.simple
  implicit val deployedTopologyApplyStateChange: ApplyStateChange[DeployedTopology, DeployedTopologyStateChange.StateChange] = ApplyStateChange.simple
}
