/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy.persistence

import com.mwam.kafkakewl.domain.*
import zio.Task

trait PersistentStore {
  def loadAll(): Task[Seq[TopologyDeployment]]
  def save(topologyDeployments: Map[TopologyId, TopologyDeployment]): Task[Unit]
}
