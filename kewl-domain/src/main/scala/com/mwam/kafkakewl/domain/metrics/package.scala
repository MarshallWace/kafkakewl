/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import scala.collection.SortedMap

package object metrics {
  type TopicPartitionsConsumerGroupMetrics = SortedMap[String, SortedMap[Int, ConsumerGroupMetrics]]
}
