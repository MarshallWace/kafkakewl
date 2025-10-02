/*
 * SPDX-FileCopyrightText: 2024 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.services

import arrow.core.Either
import arrow.core.raise.either
import com.mwam.kafkakewl.common.config.KafkaClientConfig
import com.mwam.kafkakewl.common.config.toKafkaClientConfig
import com.mwam.kafkakewl.metrics.TopicInfoSourceConfig
import com.mwam.kafkakewl.metrics.domain.KafkaTopicPartition
import com.mwam.kafkakewl.metrics.domain.KafkaTopicPartitionInfo
import com.mwam.kafkakewl.metrics.domain.KafkaTopicPartitionInfoChanges
import com.mwam.kafkakewl.metrics.domain.KafkaTopicPartitionInfos
import com.mwam.kafkakewl.metrics.domain.diff
import com.mwam.kafkakewl.utils.toEither
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.BytesDeserializer
import org.apache.kafka.common.utils.Bytes
import kotlin.time.Clock
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue
import kotlin.time.toJavaDuration

interface KafkaTopicInfoSource : AutoCloseable {
    fun topicInfoChanges(): SharedFlow<KafkaTopicPartitionInfoChanges>
    fun startPublishing(): Unit
}

private val logger = KotlinLogging.logger {}

class KafkaTopicInfoSourceImpl(
    kafkaClientConfig: KafkaClientConfig,
    private val config: TopicInfoSourceConfig,
) : KafkaTopicInfoSource {
    private val javaTimeout: java.time.Duration = config.timeout.toJavaDuration()
    private val excludeTopicRegexes: List<Regex> = config.excludeTopicRegexes.map { it.toRegex() }
    private val consumer: KafkaConsumer<Bytes, Bytes>
    private val sharedFlow = MutableSharedFlow<KafkaTopicPartitionInfoChanges>()
    private val scope = CoroutineScope(Dispatchers.Default)
    private var topicInfoPollingJob: Job? = null

    init {
        val consumerConfig = kafkaClientConfig.toKafkaClientConfig()
        consumer = KafkaConsumer<Bytes, Bytes>(consumerConfig, BytesDeserializer(), BytesDeserializer())
    }

    override fun topicInfoChanges(): SharedFlow<KafkaTopicPartitionInfoChanges> = sharedFlow

    override fun startPublishing() {
        topicInfoPollingJob = scope.launch {
            var lastTopicPartitionInfos: KafkaTopicPartitionInfos? = null
            while (isActive) {
                val (topicPartitionInfos, queryTopicPartitionInfosDuration) = measureTimedValue { queryTopicPartitionInfos() }

                topicPartitionInfos.fold(
                    { error ->
                        logger.info { "Polling topic infos finished in ${queryTopicPartitionInfosDuration.inWholeMilliseconds} ms, but $error" }
                    },
                    { topicPartitionInfos ->
                        val topicPartitionInfoChanges = if (lastTopicPartitionInfos != null) {
                            lastTopicPartitionInfos.diff(topicPartitionInfos)
                        } else {
                            KafkaTopicPartitionInfoChanges(topicPartitionInfos, emptySet())
                        }
                        lastTopicPartitionInfos = topicPartitionInfos

                        logger.info { "Polling topic infos finished in ${queryTopicPartitionInfosDuration.inWholeMilliseconds} ms, returned ${topicPartitionInfos.size} topic-partition-infos: $topicPartitionInfoChanges" }
                        if (!topicPartitionInfoChanges.isEmpty) {
                            sharedFlow.emit(topicPartitionInfoChanges)
                        }
                    }
                )

                delay(config.pollInterval)
            }
        }
    }

    override fun close() {
        logger.info { "Closing..." }
        runBlocking {
            scope.cancel()
            topicInfoPollingJob?.join()
        }
        consumer.close()
        logger.info { "Finished closing" }
    }

    private fun queryTopicPartitionInfos(): Either<String, KafkaTopicPartitionInfos> = either {
        val offsetTimestamp = Clock.System.now()
        val topicPartitionInfos = consumer.runCatching { listTopics(javaTimeout) }.toEither { "listTopics() failed: ${it.message}" }.bind()

        // filtering the topics' partition infos to the ones that we should not exclude
        val filteredTopicPartitionInfos = if (excludeTopicRegexes.isNotEmpty()) {
            topicPartitionInfos.filter { topicWithPartitionInfos -> excludeTopicRegexes.none { it.matches(topicWithPartitionInfos.key!!) } }
        } else {
            topicPartitionInfos
        }

        val topicPartitions = filteredTopicPartitionInfos.values.flatMap { it.map { TopicPartition(it.topic(), it.partition()) } }

        val beginningOffsets = consumer.runCatching { consumer.beginningOffsets(topicPartitions, javaTimeout) }.toEither { "beginningOffsets(${topicPartitions.size} topic-partitions) failed: ${it.message}" } .bind()
        val endOffsets = consumer.runCatching { consumer.endOffsets(topicPartitions, javaTimeout) }.toEither { "endOffsets(${topicPartitions.size} topic-partitions) failed: ${it.message}" } .bind()

        // Logging the missing beginning / end offsets
        fun Set<TopicPartition?>.logMissingOffsets(offsetType: String): Unit {
            if (this.isNotEmpty()) {
                logger.warn { "Could not get $offsetType-offsets for topic-partitions: $this" }
            }
        }
        val topicPartitionSet = topicPartitions.toSet()
        topicPartitionSet.subtract(beginningOffsets.keys).logMissingOffsets("beginning")
        topicPartitionSet.subtract(endOffsets.keys).logMissingOffsets("end")

        // Finally returning the ones with both beginning and end offsets
        beginningOffsets.keys.intersect(endOffsets.keys)
            .associate { KafkaTopicPartition(it) to KafkaTopicPartitionInfo(beginningOffsets[it]!!, endOffsets[it]!!, offsetTimestamp) }
    }
}
