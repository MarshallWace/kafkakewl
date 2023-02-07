/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.utils

import java.nio.file._

import com.typesafe.scalalogging.LazyLogging

object WaitForFile extends LazyLogging {
  def filePath(filePath: String, waitMs: Int = 1000): Unit = {
    if (filePath != null && filePath.trim.nonEmpty) {
      while (!Files.isRegularFile(Paths.get(filePath))) {
        logger.info(s"$filePath does not exist, waiting $waitMs ms...")
        Thread.sleep(waitMs)
      }
      logger.info(s"$filePath exists.")
      // waiting some more to make sure...
      Thread.sleep(waitMs)
    }
  }
}
