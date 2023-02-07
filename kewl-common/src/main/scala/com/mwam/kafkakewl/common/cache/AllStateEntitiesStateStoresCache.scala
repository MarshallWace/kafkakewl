/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.cache

import com.mwam.kafkakewl.common.AllStateEntities

trait AllStateEntitiesStateStoresCache {
  def update(stateStores: AllStateEntities.ReadableVersionedStateStores): Unit
}
