/*
 * SPDX-FileCopyrightText: 2025 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.persistence

import com.mwam.kafkakewl.domain.TopologyDeployments
import kotlinx.coroutines.flow.Flow

interface PersistentStore {
    fun loadLatest(): Result<TopologyDeployments>
    fun save(topologyDeployments: TopologyDeployments): Result<Unit>
    fun stream(compactHistory: Boolean): Flow<TopologyDeployments>
}
