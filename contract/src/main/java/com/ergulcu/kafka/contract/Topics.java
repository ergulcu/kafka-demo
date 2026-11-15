// Copyright 2024 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package com.ergulcu.kafka.contract;

/** Default topic names; both applications can override them with {@code app.kafka.topic}. */
public final class Topics {

    public static final String MESSAGES = "demo.messages";

    /** Records the consumer could not handle after its retries end up here. */
    public static final String DEAD_LETTER_SUFFIX = "-dlt";

    private Topics() {
    }

    public static String deadLetterOf(String topic) {
        return topic + DEAD_LETTER_SUFFIX;
    }
}
