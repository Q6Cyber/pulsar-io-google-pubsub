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
package org.apache.pulsar.ecosystem.io.pubsub.integrations;

import com.google.cloud.pubsub.v1.Subscriber;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.ecosystem.io.pubsub.PubsubConnectorConfig;
import org.junit.Assert;
import org.junit.Test;

/**
 * Integration tests for {@link org.apache.pulsar.ecosystem.io.pubsub.PubsubSink}.
 */
@Slf4j
public class PubsubSinkWithStringSchemaIntegrationTest {
    private static final String PULSAR_TOPIC = "test-pubsub-sink-topic";
    private static final String PULSAR_PRODUCER_NAME = "test-pubsub-sink-producer";
    private static final String MSG = "hello-message-";
    private static final int SEND_COUNT = 1;
    private final LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(SEND_COUNT);
    private Subscriber pubsubSubscriber;

    @Test
    public void testPubsubSinkPushMessageToPubsub() throws IOException, InterruptedException {
        // setup the pub/sub subscriber
        String projectId = "pulsar-io-google-pubsub";
        String topicId = "test-pubsub-sink";
        String credential = "";
        String endpoint = "localhost:8085";

        Map<String, Object> properties = new HashMap<>();
        properties.put("pubsubEndpoint", endpoint);
        properties.put("pubsubProjectId", projectId);
        properties.put("pubsubCredential", credential);
        properties.put("pubsubTopicId", topicId);

        pubsubSubscriber = PubsubConnectorConfig.load(properties, null).newSubscriber(((pubsubMessage, ackReplyConsumer) -> {
            try {
                String data = pubsubMessage.getData().toStringUtf8();
                Assert.assertTrue(data.startsWith(MSG));
                queue.put(data);
                ackReplyConsumer.ack();
            } catch (Exception e) {
                ackReplyConsumer.nack();
                Assert.assertNull("add the pubsubMessage to queue should not throw exception", e);
            }
        }));
        // send test messages to Pulsar
        try {
            produceMessagesToPulsar();
        } catch (Exception e) {
            Assert.assertNull("produce test messages to pulsar should not throw exception", e);
        }

        // test if sink pushed message to sqs successfully
        validateSinkResult();

        // clean up
        cleanupPubsubSubscriber();
    }

    private void cleanupPubsubSubscriber() {
        pubsubSubscriber.stopAsync();
    }

    private void produceMessagesToPulsar() throws Exception {
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();

        @Cleanup
        Producer<String> pulsarProducer = pulsarClient.newProducer(Schema.STRING)
                .topic(PULSAR_TOPIC)
                .producerName(PULSAR_PRODUCER_NAME)
                .create();

        for (int i = 0; i < SEND_COUNT; i++) {
            String message = MSG + i;
            pulsarProducer.send(message);
        }
        System.out.println("send data to pulsar successfully");

        pulsarProducer.close();
        pulsarClient.close();
    }

    private void validateSinkResult() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        ScheduledFuture<Boolean> result = executorService.schedule(() -> queue.size() == SEND_COUNT, 10,
                TimeUnit.SECONDS);
        try {
            Assert.assertTrue(String.format("validate sink result failed, expected to receive %s messages, actually "
                    + "only %s messages", SEND_COUNT, queue.size()), result.get());
        } catch (Exception e) {
            Assert.assertNull("validate sink result should not throw exception", e);
        }
    }
}
