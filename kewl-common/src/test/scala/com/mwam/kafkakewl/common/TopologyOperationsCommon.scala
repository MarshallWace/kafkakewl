/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import com.mwam.kafkakewl.common.topology.TopologyLikeOperations
import com.mwam.kafkakewl.domain.topology._

trait TopologyOperationsCommon extends TopologyLikeOperations[Topology.Node, Topology.Topic, Topology.Application, Topology.RelationshipProperties] {
  def allNodesMap(topologies: Topology*): Map[NodeId, TopologyNode] = allNodesMapOfTopologies(topologies)

  def topicNode(topology: Topology, localTopicId: String, specific: Boolean = true): ResolvedTopologyLikeNode[NodeId, Topology.Node] =
    TopologyLikeNode(
      topology.topologyEntityId,
      topology.topologyNamespace,
      TopologyLikeNodeType.Topic,
      topology.fullyQualifiedNodeId(LocalNodeId(localTopicId)),
      topology.topics(LocalTopicId(localTopicId)): Topology.Node
    ).toResolved(specific)

  def applicationNode(topology: Topology, localApplicationId: String, specific: Boolean = true): ResolvedTopologyLikeNode[NodeId, Topology.Node] =
    TopologyLikeNode(
      topology.topologyEntityId,
      topology.topologyNamespace,
      TopologyLikeNodeType.Application,
      topology.fullyQualifiedNodeId(LocalNodeId(localApplicationId)),
      topology.applications(LocalApplicationId(localApplicationId)): Topology.Node
    ).toResolved(specific)

  private def localRelationship(
    topology: Topology,
    node1LocalId: String,
    node1Specific: Boolean,
    node2LocalId: String,
    node2Specific: Boolean,
    relationshipType: RelationshipType,
    relationshipProperties: Topology.RelationshipProperties
  ) = {
    def node(localNodeId: String, specific: Boolean) = {
      topology.topics.get(LocalTopicId(localNodeId))
        .map(node => ResolvedTopologyLikeNode(
          TopologyLikeNode[NodeId, Topology.Node](topology.topologyEntityId, topology.topologyNamespace, TopologyLikeNodeType.Topic, topology.fullyQualifiedNodeId(LocalNodeId(localNodeId)), node),
          specific)
        )
        .orElse(
          topology.applications.get(LocalApplicationId(localNodeId))
            .map(node => ResolvedTopologyLikeNode(
              TopologyLikeNode[NodeId, Topology.Node](topology.topologyEntityId, topology.topologyNamespace, TopologyLikeNodeType.Application, topology.fullyQualifiedNodeId(LocalNodeId(localNodeId)), node),
              specific)
            )
        ).get
    }

    TopologyLike.TopologyNodeNodeRelationship(
      node(node1LocalId, node1Specific),
      node(node2LocalId, node2Specific),
      relationshipType,
      relationshipProperties
    )
  }

  private def relationship(
    node1Topology: Topology,
    node1LocalId: String,
    node1Specific: Boolean,
    node2Topology: Topology,
    node2LocalId: String,
    node2Specific: Boolean,
    relationshipType: RelationshipType,
    relationshipProperties: Topology.RelationshipProperties
  ) = {
    def node(topology: Topology, localNodeId: String, specific: Boolean) = {
      topology.topics.get(LocalTopicId(localNodeId))
        .map(node => ResolvedTopologyLikeNode(
          TopologyLikeNode[NodeId, Topology.Node](topology.topologyEntityId, topology.topologyNamespace, TopologyLikeNodeType.Topic, topology.fullyQualifiedNodeId(LocalNodeId(localNodeId)), node),
          specific)
        )
        .orElse(
          topology.applications.get(LocalApplicationId(localNodeId))
            .map(node => ResolvedTopologyLikeNode(
              TopologyLikeNode[NodeId, Topology.Node](topology.topologyEntityId, topology.topologyNamespace, TopologyLikeNodeType.Application, topology.fullyQualifiedNodeId(LocalNodeId(localNodeId)), node),
              specific)
            )
        ).get
    }

    TopologyLike.TopologyNodeNodeRelationship(
      node(node1Topology, node1LocalId, node1Specific),
      node(node2Topology, node2LocalId, node2Specific),
      relationshipType,
      relationshipProperties
    )
  }

  def localConsumeRelationship(
    topology: Topology,
    node1LocalId: String,
    node1Specific: Boolean,
    node2LocalId: String,
    node2Specific: Boolean,
    relationshipProperties: Topology.RelationshipProperties = Topology.RelationshipProperties()
  ): TopologyLike.TopologyNodeNodeRelationship[Topology.Node, Topology.RelationshipProperties] = localRelationship(topology, node1LocalId, node1Specific, node2LocalId, node2Specific, RelationshipType.Consume(), relationshipProperties)

  def localProduceRelationship(
    topology: Topology,
    node1LocalId: String,
    node1Specific: Boolean,
    node2LocalId: String,
    node2Specific: Boolean,
    relationshipProperties: Topology.RelationshipProperties = Topology.RelationshipProperties()
  ): TopologyLike.TopologyNodeNodeRelationship[Topology.Node, Topology.RelationshipProperties] = localRelationship(topology, node1LocalId, node1Specific, node2LocalId, node2Specific, RelationshipType.Produce(), relationshipProperties)

  def consumeRelationship(
    node1Topology: Topology,
    node1LocalId: String,
    node1Specific: Boolean,
    node2Topology: Topology,
    node2LocalId: String,
    node2Specific: Boolean,
    relationshipProperties: Topology.RelationshipProperties = Topology.RelationshipProperties()
  ): TopologyLike.TopologyNodeNodeRelationship[Topology.Node, Topology.RelationshipProperties] = relationship(node1Topology, node1LocalId, node1Specific, node2Topology, node2LocalId, node2Specific, RelationshipType.Consume(), relationshipProperties)

  def produceRelationship(
    node1Topology: Topology,
    node1LocalId: String,
    node1Specific: Boolean,
    node2Topology: Topology,
    node2LocalId: String,
    node2Specific: Boolean,
    relationshipProperties: Topology.RelationshipProperties = Topology.RelationshipProperties()
  ): TopologyLike.TopologyNodeNodeRelationship[Topology.Node, Topology.RelationshipProperties] = relationship(node1Topology, node1LocalId, node1Specific, node2Topology, node2LocalId, node2Specific, RelationshipType.Produce(), relationshipProperties)
}
