/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.kafka.utils

final case class KafkaClusterInfo(
  id: String,
  connectionInfo: KafkaConnectionInfo,
  name: Option[String] = None
)
