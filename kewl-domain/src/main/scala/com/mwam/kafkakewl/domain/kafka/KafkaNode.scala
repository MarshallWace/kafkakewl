/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package kafka

final case class KafkaNode(
  id: Int,
  host: String,
  port: Int,
  rack: Option[String] = None
)
object KafkaNode {
  def apply(node: org.apache.kafka.common.Node): KafkaNode =
    new KafkaNode(
      node.id,
      node.host,
      node.port,
      Option(node.rack)
    )
}