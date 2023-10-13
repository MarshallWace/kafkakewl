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
  given JsonEncoder[TopologyDeploymentStatus] = DeriveJsonEncoder.gen[TopologyDeploymentStatus]
  given JsonDecoder[TopologyDeploymentStatus] = DeriveJsonDecoder.gen[TopologyDeploymentStatus]
  given JsonEncoder[TopologyDeployment] = DeriveJsonEncoder.gen[TopologyDeployment]
  given JsonDecoder[TopologyDeployment] = DeriveJsonDecoder.gen[TopologyDeployment]
  given JsonEncoder[DeploymentsSuccess] = DeriveJsonEncoder.gen[DeploymentsSuccess]
  given JsonDecoder[DeploymentsSuccess] = DeriveJsonDecoder.gen[DeploymentsSuccess]
  given JsonEncoder[DeploymentsFailure.NotFound] = DeriveJsonEncoder.gen[DeploymentsFailure.NotFound]
  given JsonDecoder[DeploymentsFailure.NotFound] = DeriveJsonDecoder.gen[DeploymentsFailure.NotFound]
  given JsonEncoder[DeploymentsFailure.Authorization] = DeriveJsonEncoder.gen[DeploymentsFailure.Authorization]
  given JsonDecoder[DeploymentsFailure.Authorization] = DeriveJsonDecoder.gen[DeploymentsFailure.Authorization]
  given JsonEncoder[DeploymentsFailure.Validation] = DeriveJsonEncoder.gen[DeploymentsFailure.Validation]
  given JsonDecoder[DeploymentsFailure.Validation] = DeriveJsonDecoder.gen[DeploymentsFailure.Validation]
  given JsonEncoder[DeploymentsFailure.Deployment] = DeriveJsonEncoder.gen[DeploymentsFailure.Deployment]
  given JsonDecoder[DeploymentsFailure.Deployment] = DeriveJsonDecoder.gen[DeploymentsFailure.Deployment]
  given JsonEncoder[DeploymentsFailure.Persistence] = DeriveJsonEncoder.gen[DeploymentsFailure.Persistence]
  given JsonDecoder[DeploymentsFailure.Persistence] = DeriveJsonDecoder.gen[DeploymentsFailure.Persistence]
}
