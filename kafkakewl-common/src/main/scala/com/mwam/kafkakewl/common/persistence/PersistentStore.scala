/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.persistence

import com.mwam.kafkakewl.domain.*
import zio.*
import zio.stream.*

trait PersistentStore {
  def loadLatest(): Task[TopologyDeployments]
  def save(topologyDeployments: TopologyDeployments): Task[Unit]
  def stream(compactHistory: Boolean): Stream[Throwable, TopologyDeployments]
}
