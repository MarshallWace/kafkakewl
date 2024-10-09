/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.migrate

import io.circe.Json

final case class VnextDeploymentOptions(dryRun: Boolean = false, allowUnsafe: Boolean = false)
final case class VnextDeployment(
  options: VnextDeploymentOptions = VnextDeploymentOptions(),
  deploy: Seq[Json],
  delete: Seq[String]
)
