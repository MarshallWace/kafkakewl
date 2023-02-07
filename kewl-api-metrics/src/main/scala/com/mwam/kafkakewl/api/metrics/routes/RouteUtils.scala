/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.routes

import nl.grons.metrics4.scala.{DefaultInstrumented, MetricName}

trait RouteUtils extends DefaultInstrumented {
  override lazy val metricBaseName = MetricName("com.mwam.kafkakewl.api.metrics.route")
}
