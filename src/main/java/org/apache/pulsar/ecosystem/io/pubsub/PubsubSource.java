/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.ecosystem.io.pubsub;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;

/**
 * PubsubSource feed data from Google Cloud Pub/Sub into Pulsar.
 */
@Slf4j
public class PubsubSource extends PubsubConnector implements Source<byte[]> {
    private static final int DEFAULT_QUEUE_LENGTH = 1000;
    private static final String METRICS_TOTAL_SUCCESS = "_pubsub_source_total_success_";
    private static final String METRICS_TOTAL_FAILURE = "_pubsub_source_total_failure_";
    private SourceContext sourceContext;
    private LinkedBlockingQueue<PubsubRecord> queue;
    private Subscriber subscriber;
    private ExecutorService executorService;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        this.sourceContext = sourceContext;
        initialize(config, sourceContext);

        queue = new LinkedBlockingQueue<>(DEFAULT_QUEUE_LENGTH);
        this.executorService = Executors.newFixedThreadPool(1);
        executorService.execute(() -> {
            try {
                this.subscriber = this.getConfig().newSubscriber(this::receiveMessage);
                log.info("listening for messages on {}", this.subscriber.getSubscriptionNameString());
            } catch (Exception e) {
                log.error("encountered errors while starting the subscriber", e);
            }
        });
    }

    private void receiveMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
        PubsubRecord data = new PubsubRecord(this.sourceContext.getOutputTopic(), pubsubMessage,
                ackReplyConsumer);
        try {
            boolean successfullyInserted = queue.offer(data, 300, TimeUnit.MILLISECONDS);
            if (!successfullyInserted) {
                failRecurd(data);
                log.error("unable to insert message into queue from Google Cloud Pub/Sub");
            }
        } catch (InterruptedException e) {
            failRecurd(data);
            log.error("encountered errors when receive message from Google Cloud Pub/Sub", e);
        }
    }

    private void failRecurd(PubsubRecord record) {
        record.getAckReplyConsumer().nack();
        if (sourceContext != null) {
            sourceContext.recordMetric(METRICS_TOTAL_FAILURE, 1);
        }
    }

    @Override
    public Record<byte[]> read() throws Exception {
        PubsubRecord record = this.queue.take();
        record.getAckReplyConsumer().ack();
        sourceContext.recordMetric(METRICS_TOTAL_SUCCESS, 1);
        return record;
    }

    @Override
    public void close() {
        if (queue != null && !queue.isEmpty()) {
            queue.forEach(pubsubRecord -> pubsubRecord.getAckReplyConsumer().nack());
        }
        if (this.subscriber != null) {
            this.subscriber.stopAsync().awaitTerminated();
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(3000, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
                // wait a while for tasks to respond to being cancelled
                executorService.awaitTermination(3000, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.info("PubsubSource closed.");
    }
}
