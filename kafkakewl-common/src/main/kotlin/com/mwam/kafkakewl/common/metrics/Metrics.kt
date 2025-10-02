/*
 * SPDX-FileCopyrightText: 2025 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.metrics

fun metricsName(name: String): String {
    return "kafkakewl_vnext_$name";
}
