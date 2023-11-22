/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package zio.metrics.connectors

import zio._
import zio.metrics.connectors.internal.MetricsClient

package object timelessprometheus {

  lazy val publisherLayer: ULayer[TimelessPrometheusPublisher] =
    ZLayer.fromZIO(TimelessPrometheusPublisher.make)

  lazy val prometheusLayer
      : ZLayer[MetricsConfig & TimelessPrometheusPublisher, Nothing, Unit] =
    ZLayer.fromZIO(
      for {
        pub <- ZIO.service[TimelessPrometheusPublisher]
        _ <- MetricsClient.make(prometheusHandler(pub))
      } yield ()
    )

  private def prometheusHandler(
      clt: TimelessPrometheusPublisher
  ): Iterable[MetricEvent] => UIO[Unit] =
    events =>
      for {
        old <- clt.get
        reportComplete <- ZIO.foreach(Chunk.fromIterable(events)) { e =>
          TimelessPrometheusEncoder.encode(e).catchAll { _ =>
            ZIO.succeed(Chunk.empty)
          }
        }
        groupedReport <- ZIO.succeed(groupMetricByType(reportComplete))
        _ <- clt.set(
          groupedReport.flatten
            .addString(new StringBuilder(old.length), "\n")
            .toString()
        )
      } yield ()

  def groupMetricByType(report: Chunk[Chunk[String]]): Chunk[Chunk[String]] =
    Chunk.fromIterable(
      report.groupBy(thm => thm.take(2)).map { case (th, thmChunk) =>
        th ++ thmChunk.map(_.drop(2)).flatten
      }
    )
}
