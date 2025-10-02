/*
 * SPDX-FileCopyrightText: 2025 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

fun List<Topology>.toMapById(topologies: List<Topology>): Topologies = topologies.associateBy { it.id }
