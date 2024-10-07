package com.mwam.kafkakewl.domain

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/** Base interface for the different kind of matchers that can match a fully qualified topic or application id. */
@Serializable
sealed interface Matcher
object Matchers {
    /** It matches any topic or application id */
    @Serializable
    @SerialName("any")
    object Any: Matcher

    /** It matches exactly the specified topic or application id */
    @Serializable
    @SerialName("exact")
    data class Exact(val value: String): Matcher

    /** It matches exactly the specified topic or application id */
    @Serializable
    @SerialName("regex")
    data class Regex(val value: String): Matcher

    /** It matches the topic or application ids in the specified namespace */
    @Serializable
    @SerialName("namespace")
    data class Namespace(val value: String): Matcher
}
