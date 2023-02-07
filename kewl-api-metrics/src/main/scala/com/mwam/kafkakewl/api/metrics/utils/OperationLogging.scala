/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.utils

import com.mwam.kafkakewl.api.metrics.services.KafkaClusterService
import com.mwam.kafkakewl.utils.MdcUtils
import com.typesafe.scalalogging.Logger

trait OperationLogging extends MdcUtils {
  protected val logger: Logger

  def numberOfItems[T](itemType: String, s: Iterable[T]): String = s"[${s.size} ${itemType}s]"

  def truncateToHeadTail[T](itemType: String, s: Seq[T], limit: Int = 4, separator: String = ", "): String = {
    if (s.nonEmpty && limit == 0) {
      numberOfItems(itemType, s)
    } else if (s.size > limit) {
      val limitHead = limit / 2
      val limitTail = limit - limitHead
      s"[${s.take(limitHead).mkString(", ")}, ...${s.size - limit} more ${itemType}s..., ${s.slice(s.size - limitTail, s.size).mkString(", ")}]"
    } else {
      s.mkString("[", ", ", "]")
    }
  }

  def withLogging[T](logString: String)(expr: => KafkaClusterService.Result[T]): KafkaClusterService.Result[T] = {
    val result = expr
    result match {
      case Right(r) => logger.info(s"$logString: $r")
      case Left(e) => logger.warn(s"$logString: $e")
    }
    result
  }

  def withLoggingMultiple[C <: Iterable[_]](logString: String, resultType: String, limit: Int = 4)(expr: => KafkaClusterService.Result[C]): KafkaClusterService.Result[C] = {
    val result = expr
    result match {
      case Right(r) => logger.info(s"$logString: ${truncateToHeadTail(resultType, r.toSeq, limit, separator = ";")}")
      case Left(e) => logger.warn(s"$logString: $e")
    }
    result
  }

  def withLoggingMultipleKeyed[C <: Iterable[(_, KafkaClusterService.Result[_])]](logString: String, resultType: String, limitPerError: Int = 4)(expr: => C): C = {
    val result = expr

    // split up the result into successful results and errors
    val (results, errors) = result.partition { case (_, r) => r.isRight }

    // not logging the successful results, only their count
    logger.info(s"$logString: ${numberOfItems(resultType, results)}")

    // logging errors separately, grouping them by the error message (there aren't that many different ones)
    // and for every error shorten the list of keys using limitPerError
    val errorStrings = errors
      .collect { case (key, Left(e)) => (e, key) }
      .groupBy { case (e, _) => e }
      .map {
        case (e, keys) =>
          val keysString = truncateToHeadTail(
            resultType,
            keys.map { case (_, key) => key.toString }.toSeq,
            limitPerError
          )
          s"$e: $keysString"
      }
      .toSeq

    if (errorStrings.nonEmpty) {
      logger.warn(s"$logString: ${errorStrings.mkString("[", ";", "]")}")
    }

    result
  }
}
