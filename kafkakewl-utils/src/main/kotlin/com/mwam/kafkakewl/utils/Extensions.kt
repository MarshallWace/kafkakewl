/*
 * SPDX-FileCopyrightText: 2024 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.utils

import arrow.core.Either

fun <A> Result<A>.toEither(): Either<Throwable, A> =
    fold(
        onSuccess = { Either.Right(it) },
        onFailure = { Either.Left(it) }
    )

fun <Error, A> Result<A>.toEither(f: (Throwable) -> Error): Either<Error, A> =
    fold(
        onSuccess = { Either.Right(it) },
        onFailure = { Either.Left(f(it)) }
    )
