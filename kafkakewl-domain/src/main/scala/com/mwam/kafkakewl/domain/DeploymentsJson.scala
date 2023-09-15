/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder}

object DeploymentsJson {
  import TopologyJson.given

  given JsonEncoder[DeploymentOptions] = DeriveJsonEncoder.gen[DeploymentOptions]
  given JsonDecoder[DeploymentOptions] = DeriveJsonDecoder.gen[DeploymentOptions]

  given JsonEncoder[Deployments] = DeriveJsonEncoder.gen[Deployments]
  given JsonDecoder[Deployments] = DeriveJsonDecoder.gen[Deployments]

  given JsonEncoder[DeploymentsResult] = DeriveJsonEncoder.gen[DeploymentsResult]
  given JsonDecoder[DeploymentsResult] = DeriveJsonDecoder.gen[DeploymentsResult]

  given JsonEncoder[TopologyDeploymentStatus] = DeriveJsonEncoder.gen[TopologyDeploymentStatus]
  given JsonDecoder[TopologyDeploymentStatus] = DeriveJsonDecoder.gen[TopologyDeploymentStatus]

  given JsonEncoder[TopologyDeployment] = DeriveJsonEncoder.gen[TopologyDeployment]
  given JsonDecoder[TopologyDeployment] = DeriveJsonDecoder.gen[TopologyDeployment]
}
