/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.utils

object CollectionExtensions {
  extension [Key](map: Map[Key, Long]) {
    def add(other: Map[Key, Long]): Map[Key, Long] = {
      (map.keySet union other.keySet)
        .map(key => (key, map.getOrElse(key, 0L) + other.getOrElse(key, 0L)))
        .toMap
    }

    def subtract(other: Map[Key, Long]): Map[Key, Long] = {
      (map.keySet union other.keySet)
        .map(key => (key, map.getOrElse(key, 0L) - other.getOrElse(key, 0L)))
        .toMap
    }
  }
}
