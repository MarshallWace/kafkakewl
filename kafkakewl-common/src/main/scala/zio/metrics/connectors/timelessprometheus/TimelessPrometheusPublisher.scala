/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package zio.metrics.connectors.timelessprometheus

import zio._

class TimelessPrometheusPublisher private (current: Ref[String]) {

  def get(implicit trace: Trace): UIO[String] =
    current.get

  def set(next: String)(implicit trace: Trace): UIO[Unit] =
    current.set(next)
}

object TimelessPrometheusPublisher {

  def make: ZIO[Any, Nothing, TimelessPrometheusPublisher] = for {
    current <- Ref.make[String]("")
  } yield new TimelessPrometheusPublisher(current)

}
