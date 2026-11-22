// Copyright 2024 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package com.ergulcu.kafkaproducer.config;

import com.ergulcu.kafka.contract.Topics;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * @param topic       where the messages go
 * @param partitions  partitions of the topic when the producer creates it
 * @param sendTimeout how long a request waits for the broker to acknowledge the record
 */
@ConfigurationProperties("app.kafka")
public record ProducerProperties(
        @DefaultValue(Topics.MESSAGES) String topic,
        @DefaultValue("3") int partitions,
        @DefaultValue("10s") Duration sendTimeout) {
}
