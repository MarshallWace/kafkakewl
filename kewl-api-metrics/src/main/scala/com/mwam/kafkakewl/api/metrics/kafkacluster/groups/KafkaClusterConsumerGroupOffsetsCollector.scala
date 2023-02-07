/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster.groups

/**
 * Base trait for consumer-group offsets collectors.
 */
trait KafkaClusterConsumerGroupOffsetsCollector {
  def stop(): Unit
}
