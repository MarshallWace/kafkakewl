/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl

import java.util.concurrent.{ExecutionException, TimeUnit, Future => JavaFuture}
import com.codahale.metrics.Gauge
import com.typesafe.config.{Config, ConfigValueType}
import com.typesafe.scalalogging.Logger
import nl.grons.metrics4.scala.MetricBuilder

import scala.collection.JavaConverters._
import scala.collection.SortedMap
import scala.concurrent._
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.control.ControlThrowable
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

package object utils {
  def failFast(status: Int = 1): Unit = {
    Thread.sleep(5000) // just so that the log reaches its destination
    System.exit(status)
  }

  object Using {
    def apply[C <: AutoCloseable, R](c: C)(op: C => R): R = {
      try {
        op(c)
      } finally {
        c.close()
      }
    }
  }

  implicit class Tuple2IterableExtensions[K, V](tuples: Iterable[(K, V)]) {
    def toSortedMap(implicit ord: Ordering[K]): SortedMap[K, V] = SortedMap.empty[K, V] ++ tuples
  }

  def durationOf[T](action: => T): (T, FiniteDuration) = {
    val beforeNanoTime = System.nanoTime()
    val result = action
    val duration = Duration.fromNanos(System.nanoTime() - beforeNanoTime)
    (result, duration)
  }

  def withDurationOf[T](action: => T)(handler: Duration => Unit): T = {
    val (r, d) = durationOf[T](action)
    handler(d)
    r
  }

  def durationBetween(fromNanos: Long, toNanos: Long): FiniteDuration = Duration.fromNanos(toNanos - fromNanos)
  def durationSince(nanos: Long): FiniteDuration = durationBetween(nanos, System.nanoTime())

  implicit class DurationExtensions(duration: FiniteDuration) {
    def toMillisDouble: Double = duration.toUnit(TimeUnit.MILLISECONDS)
    def toSecondsDouble: Double = duration.toUnit(TimeUnit.SECONDS)
  }

  implicit class ThrowableExtensions(t: Throwable) {
    def toErrorMessage: String =
      if (t.getMessage == null)
        s"${t.getClass.getName}"
      else
        s"${t.getClass.getName}: ${t.getMessage}"
  }

  implicit class MapExtensions[K, V](map: Map[K, V]) {
    def toPrettyString: String = if (map.size < 2) map.toString else map.mkString("Map(\n  ", "\n  ", "\n)")
  }

  implicit class IterableExtensions[T](s: Iterable[T]) {
    def duplicates: Iterable[T] = s.groupBy(identity).filter { case (_, v) => v.size > 1 }.keys.toSeq
  }

  implicit class StringExtensions(string: String) {
    def toActorNameString = string.replaceAll("[^\\w\\-_.*$+:@&=,!~';]", "_")

    def quote = s"'$string'"

    def rWhole: Regex = {
      val regexPrefix = if (string.startsWith("^")) "" else "^"
      val regexSuffix = if (string.endsWith("$")) "" else "$"
      // matching the whole value, making sure the regex is wrapped in ^ and $
      s"$regexPrefix$string$regexSuffix".r
    }

    def defaultIfNull(default: String = ""): String = if (string == null) default else string
    def emptyIfNull: String = defaultIfNull("")

    def nonNullAndEmpty: Boolean = string != null && string.nonEmpty

    def noneIfEmpty: Option[String] = if (string.length == 0) None else Some(string)
    def toIntOrNone: Option[Int] = {
      try {
        Some(string.toInt)
      } catch {
        case _: NumberFormatException => None
      }
    }
    def toLongOrNone: Option[Long] = {
      try {
        Some(string.toLong)
      } catch {
        case _: NumberFormatException => None
      }
    }
    def toDoubleOrNone: Option[Double] = {
      try {
        Some(string.toDouble)
      } catch {
        case _: NumberFormatException => None
      }
    }
    def toShortOrNone: Option[Short] = {
      try {
        Some(string.toShort)
      } catch {
        case _: NumberFormatException => None
      }
    }
    def toBooleanOrNone: Option[Boolean] = {
      try {
        Some(string.toBoolean)
      } catch {
        case _: IllegalArgumentException => None
      }
    }
  }

  implicit class ConfigExtensions(config: Config) {
    def getStringOrNone(path: String): Option[String] = if (config.hasPath(path)) Some(config.getString(path)) else None
    def getStringOrNoneIfEmpty(path: String): Option[String] = if (config.hasPath(path)) config.getString(path).noneIfEmpty else None
    def getIntOrNoneIfEmpty(path: String): Option[Int] = getStringOrNoneIfEmpty(path).flatMap(_.toIntOrNone)
    def getLongOrNoneIfEmpty(path: String): Option[Long] = getStringOrNoneIfEmpty(path).flatMap(_.toLongOrNone)
    def getDoubleOrNoneIfEmpty(path: String): Option[Double] = getStringOrNoneIfEmpty(path).flatMap(_.toDoubleOrNone)
    def getShortOrNoneIfEmpty(path: String): Option[Short] = getStringOrNoneIfEmpty(path).flatMap(_.toShortOrNone)
    def getBooleanOrNone(path: String): Option[Boolean] = if (config.hasPath(path)) config.getStringOrNoneIfEmpty(path).flatMap(_.toBooleanOrNone) else None

    def sqlDbConnectionInfo(path: String): Option[SqlDbConnectionInfo] = {
      for {
        host <- config.getStringOrNoneIfEmpty(s"$path.host")
        port = config.getIntOrNoneIfEmpty(s"$path.port")
        database <- config.getStringOrNoneIfEmpty(s"$path.database")
      } yield SqlDbConnectionInfo(host, port, database)
    }

    def sqlDbConnectionInfoOrNone(path: String): Option[SqlDbConnectionInfo] = if (config.hasPath(path)) sqlDbConnectionInfo(path) else None

    def kafkaConfig(path: String): Map[String, String] =
      if (config.hasPath(path)) {
        config.getConfig(path).root()
          .asScala
          .map { case (key, value) =>
            (
              key.trim,
              if (value.valueType() == ConfigValueType.STRING) {
                val u = value.unwrapped()
                u.toString.trim
              } else {
                value.render()
              }
            )
          }
          .filter { case (key, value) => !key.isEmpty && !value.isEmpty}
          .toMap

      } else
        Map.empty[String, String]
  }

  implicit class CommonTryExtensions[T](t: Try[T]) {
    def toRightOrErrorMessage: Either[String, T] = {
      t match {
        case Success(r) => Right(r)
        case Failure(e) => Left(e.toErrorMessage)
      }
    }
  }

  /**
    * Handles Throwable instances with the specified partial function in a safe way (not letting it handle ControlThrowable).
    */
  def safely[T](handler: PartialFunction[Throwable, T]): PartialFunction[Throwable, T] = {
    case ex: ControlThrowable => throw ex
    case ex: Throwable if handler.isDefinedAt(ex) => handler(ex)
  }

  implicit class JavaFutureExtensions[T](f: JavaFuture[T]) {
    def toTry: Try[T] = {
      try Success(f.get()) catch {
        case e: ExecutionException => Failure(e.getCause)
        case e: CancellationException => Failure(e)
        // not handling anything else, let those propagate and crash
      }
    }

    def toScala(implicit ec: ExecutionContext): Future[T] = {
      // TODO this may have problems if you have lots of these blocking threads concurrently. Would be better to poll the java future periodically.
      Future { blocking { f.get() } }
    }
  }

  implicit class ScalaFutureExtensions[T](f: Future[T]) {
    def toFutureTry(implicit ec: ExecutionContext): Future[Try[T]] = {
      val promise = Promise[Try[T]]()
      f.onComplete(x => promise.success(x))
      promise.future
    }

    def crashIfFailed(message: String = "", exitCode: Int = 1)(implicit ec: ExecutionContext): Unit = {
      f.failed.foreach { t =>
        val waitMs = 10000
        val logger = Logger(getClass)
        ApplicationMetrics.errorCounter.inc()
        logger.error("exception:", t)
        logger.error(s"crashing the process in $waitMs ms: $message")
        Thread.sleep(waitMs) // we wait a bit so that the logs can be finished
        sys.exit(exitCode)
      }
    }
  }

  implicit class MetricBuilderExtensions(metrics: MetricBuilder) {
    def metricFullName(metricName: String): String = metrics.baseName.append(metricName).name

    /**
     * Creates the gauge if it doesn't exist yet, faster than the other method below.
     *
     * The proper way of doing it would be metrics.gauge(gaugeName) {...}, but that throws an exception
     * if the gauge already exists which is not ideal if we want createIfDoesNotExist behavior
     * (99% of the time it'll throw the exception which we have to catch).
     * Checking before calling metrics.gauge() also quite slow because the API doesn't really support it (only metrics.registry.getGauges())
     *
     * The solution is to use directly the underlying MetricRegistry.gauge() method which does getOrAdd() internally.
     *
     * The disadvantage of this method is that these gauges are not registered in the MetricBuilder and hence can't be removed with
     * metrics.unregisterGauges() and the ActorInstrumentedLifeCycle also cannot be used that relies on those gauges being in the
     * MetricBuilder.
     *
     * Since we don't need metrics.unregisterGauges(), this workaround is fine.
     *
     * @param gaugeName the name of the gauge
     * @param defaultValue the default value which is emitted just before the gauge is deleted (when receiving None)
     * @param f the named parameter to calculate the current value of the gauge
     * @tparam A the type of the gauge
     */
    def createGaugeIfDoesNotExistFast[A](gaugeName: String, defaultValue: A)(f: => Option[A]): Unit = {
      metrics.registry.gauge(
        metricFullName(gaugeName),
        () => new Gauge[A] {
          def getValue: A = f.getOrElse {
            // no value, remove the gauge...
            metrics.removeGauge(gaugeName)
            // ...and return something for one last time
            defaultValue
          }
        }
      )
    }

    def createGaugeIfDoesNotExistSlow[A](gaugeName: String, defaultValue: A)(f: => Option[A]): Unit = {
      // the condition usually works, but in concurrent cases, we need to catch the exception below
      if (!gaugeExists(gaugeName)) {
        try {
          metrics.gauge(gaugeName) {
            f.getOrElse {
              // no value, remove the gauge...
              metrics.removeGauge(gaugeName)
              // ...and return something for one last time
              defaultValue
            }
          }
        } catch {
          // ignoring the "already exist exception"
          // this is ugly, but it's the simplest: unfortunately registering the gauge is different from registering the other metrics, it throws if it's already exists
          case e: java.lang.IllegalArgumentException if e.getMessage.contains("already exists") => ()
        }
      }
    }

    def gaugeExists(gaugeName: String): Boolean = {
      val gaugeFullName = metricFullName(gaugeName)
      !metrics.registry
        .getGauges { (name, _) => name == gaugeFullName }
        .isEmpty
    }

    def removeGauge(gaugeName: String): Boolean = {
      val gaugeFullName = metricFullName(gaugeName)
      metrics.registry.remove(gaugeFullName)
    }
  }
}
