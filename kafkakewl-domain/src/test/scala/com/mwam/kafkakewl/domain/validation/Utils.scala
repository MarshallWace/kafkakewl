/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain.validation

import com.mwam.kafkakewl.domain.*
import com.mwam.kafkakewl.domain.validation.*
import zio.test.*

val validAssertion = Assertion.equalTo(valid)
def invalidAssertion(error: String, errors: String*) = Assertion.equalTo(invalid(error, errors: _*))

def emptyTopology(id: String = "test", namespace: String = "") = Topology(id = TopologyId(id), namespace = Namespace(namespace))

val emptyValidTopology = emptyTopology()
