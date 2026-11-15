// Copyright 2024 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package com.ergulcu.kafkaconsumer.service;

import com.ergulcu.kafka.contract.MessageEvent;
import com.ergulcu.kafkaconsumer.config.ConsumerProperties;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/** The latest messages and dead letters, newest first, kept in memory for the demo endpoints. */
@Component
public class ReceivedMessages {

    public record Received(MessageEvent event, String topic, int partition, long offset, Instant receivedAt) {
    }

    public record DeadLetter(String key, String value, String reason, String originalTopic, Instant receivedAt) {
    }

    private final int keep;
    private final Deque<Received> messages = new ArrayDeque<>();
    private final Deque<DeadLetter> deadLetters = new ArrayDeque<>();

    public ReceivedMessages(ConsumerProperties properties) {
        this.keep = Math.max(1, properties.keep());
    }

    public synchronized void add(Received received) {
        push(messages, received);
    }

    public synchronized void addDeadLetter(DeadLetter deadLetter) {
        push(deadLetters, deadLetter);
    }

    public synchronized List<Received> messages() {
        return List.copyOf(messages);
    }

    public synchronized List<DeadLetter> deadLetters() {
        return List.copyOf(deadLetters);
    }

    private <T> void push(Deque<T> deque, T item) {
        deque.addFirst(item);
        while (deque.size() > keep) {
            deque.removeLast();
        }
    }
}
