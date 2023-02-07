/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.kafka.utils

final case class KafkaConnectionInfo(brokers: String, securityProtocol: Option[String], config: Map[String, String]) {
  def withConfig(key: String, value: String): KafkaConnectionInfo = copy(config = config + (key -> value))
}
final case class KafkaConnection(info: KafkaConnectionInfo, jaasConfig: Option[String] = None) {
  def withConfig(key: String, value: String): KafkaConnection = copy(info = info.withConfig(key, value))
}
