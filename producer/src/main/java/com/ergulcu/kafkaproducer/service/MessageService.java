// Copyright 2024 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package com.ergulcu.kafkaproducer.service;

import com.ergulcu.kafka.contract.MessageEvent;

public interface MessageService {

    /** Publishes the text and returns the event once the broker has acknowledged it. */
    MessageEvent sendMessage(String text);
}
