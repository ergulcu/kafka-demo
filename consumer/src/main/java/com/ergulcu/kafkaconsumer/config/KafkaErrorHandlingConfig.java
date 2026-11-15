// Copyright 2024 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package com.ergulcu.kafkaconsumer.config;

import com.ergulcu.kafkaconsumer.service.InvalidMessageException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * A record whose handling fails is tried again {@code app.kafka.retries} times and then published to the
 * dead letter topic, so one bad record does not block the partition. Records that can never succeed
 * ({@link InvalidMessageException}: not JSON, no text) go to the dead letter topic right away.
 * Spring Boot hands this handler to the listener container factory.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    CommonErrorHandler errorHandler(KafkaTemplate<Object, Object> template, ConsumerProperties properties) {
        // Partition -1 lets the producer pick one; the dead letter topic has fewer partitions than the source.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, exception) -> new TopicPartition(properties.deadLetterTopic(), -1));
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer,
                new FixedBackOff(properties.retryBackoff().toMillis(), properties.retries()));
        handler.addNotRetryableExceptions(InvalidMessageException.class);
        return handler;
    }
}
