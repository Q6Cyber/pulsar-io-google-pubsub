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
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.ecosystem.io.pubsub.PubsubConnectorConfig;
import org.apache.pulsar.ecosystem.io.pubsub.PubsubPublisher;
import org.apache.pulsar.ecosystem.io.pubsub.PubsubSink;
import org.apache.pulsar.ecosystem.io.pubsub.testdata.User;
import org.apache.pulsar.ecosystem.io.pubsub.util.AvroUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Integration tests for {@link PubsubSink}.
 */
@Slf4j
public class PubsubSinkWithAvroIntegrationTest {
    private static final String PULSAR_TOPIC = "test-pubsub-sink-avro-topic";
    private static final String PULSAR_PRODUCER_NAME = "test-pubsub-sink-avro-producer";
    private static final String MSG = "hello-message-";
    private static final int SEND_COUNT = 1;
    private final LinkedBlockingQueue<Object> queue = new LinkedBlockingQueue<>(SEND_COUNT);
    private Subscriber pubsubSubscriber;

    @Test
    public void testPubsubSinkPushMessageToPubsub() throws IOException, InterruptedException {
        // set up the pub/sub subscriber
        String projectId = "pulsar-io-google-pubsub";
        String topicId = "test-pubsub-sink-avro";
        String credential = "";
        String endpoint = "localhost:8085";

        Map<String, Object> properties = new HashMap<>();
        properties.put("pubsubEndpoint", endpoint);
        properties.put("pubsubProjectId", projectId);
        properties.put("pubsubCredential", credential);
        properties.put("pubsubTopicId", topicId);

        String avroSchemaString =
                "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"}]}";
        org.apache.avro.Schema schema = AvroUtils.parseSchemaString(avroSchemaString);

        pubsubSubscriber = PubsubConnectorConfig.load(properties, null).newSubscriber(((pubsubMessage, ackReplyConsumer) -> {
            try {
                GenericRecord genericRecord =
                        PubsubPublisher.deserializeAvroWithJsonEncoder(pubsubMessage.getData().toByteArray(), schema);
                Assert.assertTrue(genericRecord.get("key").toString().startsWith(MSG));
                queue.put(genericRecord);
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
        Producer<User> pulsarProducer = pulsarClient.newProducer(Schema.AVRO(User.class))
                .topic(PULSAR_TOPIC)
                .producerName(PULSAR_PRODUCER_NAME)
                .create();

        for (int i = 0; i < SEND_COUNT; i++) {
            pulsarProducer.send(User.builder().key(MSG + i).build());
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

