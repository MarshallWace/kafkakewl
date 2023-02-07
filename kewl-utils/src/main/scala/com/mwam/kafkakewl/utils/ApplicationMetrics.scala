/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.utils

import nl.grons.metrics4.scala.{Counter, DefaultInstrumented, MetricName}

object ApplicationMetrics extends DefaultInstrumented {
  override lazy val metricBaseName: MetricName = MetricName("com.mwam.kafkakewl.api.errors")
  val errorCounter: Counter = metrics.counter("errorcount")

  def initialize(): Unit = {
    // not doing anything just enforcing the initialization of this object and hence
    // making sure that the counter is created and initialized to zero
  }
}
