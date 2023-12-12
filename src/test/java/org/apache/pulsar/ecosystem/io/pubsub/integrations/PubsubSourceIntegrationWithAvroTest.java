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

import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.avro.generic.GenericData;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.ecosystem.io.pubsub.PubsubConnectorConfig;
import org.apache.pulsar.ecosystem.io.pubsub.PubsubPublisher;
import org.apache.pulsar.ecosystem.io.pubsub.util.AvroUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Integration tests for {@link org.apache.pulsar.ecosystem.io.pubsub.PubsubSource}.
 */
public class PubsubSourceIntegrationWithAvroTest {
    private static final String PULSAR_TOPIC = "test-pubsub-source-avro-topic";
    private static final String PULSAR_SUB_NAME = "test-pubsub-source-avro-validator";
    private static final String MSG = "hello-message-";
    private static final int SEND_COUNT = 100;

    @Test
    public void testPubsubSourcePushMessageToPubsub() throws IOException, ClassNotFoundException {
        try {
            produceMessagesToPubsub();
        } catch (Exception e) {
            Assert.assertNull("produce test messages to Google Cloud Pub/Sub should not throw exception", e);
        }

        validateSourceResult();
    }

    private void produceMessagesToPubsub() throws Exception {
        String projectId = "pulsar-io-google-pubsub";
        String topicId = "test-pubsub-source-avro";
        String credential = "";
        String endpoint = "localhost:8085";
        String pubsubSchemaType = "AVRO";
        String pubsubSchemaEncoding = "JSON";
        String pubsubSchemaDefinition = "{\"type\":\"record\",\"name\":\"User\",\"fields\": [{\"name\":\"key\","
                + "\"type\":\"string\"}]}";
        String pubsubSchemaId = "test-pubsub-source-avro";

        Map<String, Object> properties = new HashMap<>();
        properties.put("pubsubEndpoint", endpoint);
        properties.put("pubsubProjectId", projectId);
        properties.put("pubsubCredential", credential);
        properties.put("pubsubTopicId", topicId);
        properties.put("pubsubSchemaId", pubsubSchemaId);
        properties.put("pubsubSchemaType", pubsubSchemaType);
        properties.put("pubsubSchemaEncoding", pubsubSchemaEncoding);
        properties.put("pubsubSchemaDefinition", pubsubSchemaDefinition);

        PubsubConnectorConfig config = PubsubConnectorConfig.load(properties, null);
        PubsubPublisher publisher = PubsubPublisher.create(config);

        // wait for the subscriber to perform the subscription operation.
        Thread.sleep(3 * 1000);

        org.apache.avro.Schema schema = AvroUtils.parseSchemaString(pubsubSchemaDefinition);

        for (int i = 0; i < SEND_COUNT; i++) {
            org.apache.avro.generic.GenericRecord genericRecord = new GenericData.Record(schema);
            genericRecord.put("key", MSG + i);
            publisher.getPublisher()
                    .publish(PubsubMessage.newBuilder().setData(publisher.serializeAvroSchema(genericRecord)).build())
                    .get();
        }
        publisher.shutdown();
        System.out.println("send data to Google Cloud Pub/Sub successfully");
    }

    private void validateSourceResult() throws IOException, ClassNotFoundException {
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();

        @Cleanup
        Consumer<GenericRecord> pulsarConsumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .topic(PULSAR_TOPIC)
                .subscriptionName(PULSAR_SUB_NAME)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        int recordsNumber = 0;
        Message<GenericRecord> msg = pulsarConsumer.receive(2, TimeUnit.SECONDS);
        while (msg != null) {
            Assert.assertTrue(new String(msg.getData(), StandardCharsets.UTF_8).contains(MSG));
            pulsarConsumer.acknowledge(msg);
            recordsNumber++;
            msg = pulsarConsumer.receive(2, TimeUnit.SECONDS);
        }
        Assert.assertEquals(SEND_COUNT, recordsNumber);
        pulsarConsumer.close();
        pulsarClient.close();
    }
}
