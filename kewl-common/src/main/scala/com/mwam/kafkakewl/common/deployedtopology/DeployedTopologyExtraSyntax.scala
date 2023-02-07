/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.deployedtopology

import com.mwam.kafkakewl.common.ReadableStateStore
import com.mwam.kafkakewl.domain.deploy.DeployedTopology
import com.mwam.kafkakewl.domain.topology.{TopologyEntityId, TopologyToDeploy}
import com.mwam.kafkakewl.domain.{CommandError, ValueOrCommandErrors}

trait DeployedTopologyExtraSyntax {
  implicit class DeployedTopologyExtraSyntaxExtensions(stateStore: ReadableStateStore[DeployedTopology]) {
    def getLatestOrCommandErrors(deployedTopologyId: String): ValueOrCommandErrors[DeployedTopology] = {
      stateStore.getLatestLiveState(deployedTopologyId)
        .toRight(Seq(CommandError.validationError(s"no deployed topology '$deployedTopologyId'")))
        .map(_.entity)
    }

    def currentTopologies(): Map[TopologyEntityId, TopologyToDeploy] = {
      stateStore
        .getLatestLiveStates.map(dp => (dp.entity.topologyId, dp)).toMap
        .mapValues(_.entity.topologyWithVersion)
        .collect { case (p, Some(pv)) => (p, pv.topology) }
    }
  }
}
