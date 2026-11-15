// Copyright 2024 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package com.ergulcu.kafkaproducer.config;

import com.ergulcu.kafka.contract.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** The topics are created at startup by Spring's KafkaAdmin if they do not exist yet. */
@Configuration
public class TopicConfig {

    @Bean
    NewTopic messagesTopic(ProducerProperties properties) {
        return TopicBuilder.name(properties.topic()).partitions(properties.partitions()).replicas(1).build();
    }

    @Bean
    NewTopic deadLetterTopic(ProducerProperties properties) {
        return TopicBuilder.name(Topics.deadLetterOf(properties.topic())).partitions(1).replicas(1).build();
    }
}
