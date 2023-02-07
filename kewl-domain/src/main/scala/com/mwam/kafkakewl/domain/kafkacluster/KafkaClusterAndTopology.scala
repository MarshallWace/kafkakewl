/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package kafkacluster

import com.mwam.kafkakewl.domain.topology.TopologyEntityId

object KafkaClusterAndTopology {
  def id(kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId) = s"$kafkaClusterId/$topologyId"
  def parseId(id: String): (KafkaClusterEntityId, TopologyEntityId) = {
    val idParts = id.split('/')
    (KafkaClusterEntityId(idParts(0)), TopologyEntityId(idParts(1)))
  }
}
