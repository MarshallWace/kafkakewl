/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.extensions

trait Plugin {
  /**
    * The name of the plugin.
    */
  val name: String = getClass.getName
}
