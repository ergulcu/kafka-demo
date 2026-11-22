// Copyright 2024 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package com.ergulcu.kafkaconsumer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Lets the consumer start before the producer: KafkaAdmin creates missing topics and leaves existing ones alone. */
@Configuration
public class TopicConfig {

    @Bean
    NewTopic messagesTopic(ConsumerProperties properties) {
        return TopicBuilder.name(properties.topic()).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic deadLetterTopic(ConsumerProperties properties) {
        return TopicBuilder.name(properties.deadLetterTopic()).partitions(1).replicas(1).build();
    }
}
