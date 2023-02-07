/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.cache

import com.mwam.kafkakewl.common.ReadableStateStore
import com.mwam.kafkakewl.domain.deploy.DeployedTopology
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId

trait DeployedTopologyStateStoresCache {
  def update(stateStores: Map[KafkaClusterEntityId, ReadableStateStore[DeployedTopology]]): Unit
}
