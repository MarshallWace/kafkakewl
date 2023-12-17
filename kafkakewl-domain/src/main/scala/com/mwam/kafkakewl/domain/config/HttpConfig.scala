/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain.config

import zio.Duration

final case class HttpConfig(
    port: Int = 8080,
    timeout: Duration = Duration.fromSeconds(10)
)
