// Copyright 2024 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package com.ergulcu.kafka.contract;

import java.time.Instant;

/**
 * The value of every record on the demo topic, sent as JSON.
 *
 * @param id        unique id given by the producer
 * @param text      the message; the consumer rejects blank text and sends the record to the dead letter topic
 * @param createdAt when the producer accepted the message
 */
public record MessageEvent(String id, String text, Instant createdAt) {
}
