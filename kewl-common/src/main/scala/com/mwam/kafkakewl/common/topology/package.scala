/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import com.mwam.kafkakewl.domain.topology.{Topology, TopologyToDeploy}

package object topology {
  type TopologyOperations = TopologyLikeOperations[Topology.Node, Topology.Topic, Topology.Application, Topology.RelationshipProperties]
  type TopologyToDeployOperations = TopologyLikeOperations[TopologyToDeploy.Node, TopologyToDeploy.Topic, TopologyToDeploy.Application, TopologyToDeploy.RelationshipProperties]
}
