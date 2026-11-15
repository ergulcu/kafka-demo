// Copyright 2024 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package com.ergulcu.kafkaconsumer.config;

import com.ergulcu.kafka.contract.Topics;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * @param topic        topic to listen to; its dead letter topic is {@code <topic>-dlt}
 * @param retries      extra attempts for a record that failed for a reason other than its content
 * @param retryBackoff pause between the attempts
 * @param keep         how many received messages and dead letters {@code GET /messages} shows
 */
@ConfigurationProperties("app.kafka")
public record ConsumerProperties(
        @DefaultValue(Topics.MESSAGES) String topic,
        @DefaultValue("2") int retries,
        @DefaultValue("500ms") Duration retryBackoff,
        @DefaultValue("100") int keep) {

    public String deadLetterTopic() {
        return Topics.deadLetterOf(topic);
    }
}
