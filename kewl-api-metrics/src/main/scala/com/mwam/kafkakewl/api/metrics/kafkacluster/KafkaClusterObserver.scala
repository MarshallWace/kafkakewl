/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster

import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId

trait KafkaClusterObserver {
  def remove(kafkaClusterId: KafkaClusterEntityId): Unit
}

object KafkaClusterObserver {
  def composite(observers: KafkaClusterObserver*): KafkaClusterObserver =
    (kafkaClusterId: KafkaClusterEntityId) => observers.foreach(_.remove(kafkaClusterId))
}

