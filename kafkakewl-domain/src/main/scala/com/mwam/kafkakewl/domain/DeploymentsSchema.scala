/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import com.mwam.kafkakewl.domain.TopologySchema.given
import sttp.tapir.*
import sttp.tapir.generic.auto.*

object DeploymentsSchema {
  given Schema[Deployments] = Schema.derived[Deployments]
  given Schema[TopologyDeployment] = Schema.derived[TopologyDeployment]
  given Schema[Map[TopologyId, TopologyDeploymentStatus]] = Schema.schemaForMap[TopologyId, TopologyDeploymentStatus](_.value)
}
