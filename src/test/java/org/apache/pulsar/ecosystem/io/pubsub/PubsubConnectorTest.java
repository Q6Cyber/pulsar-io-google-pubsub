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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/**
 * Unit test for {@link PubsubConnector}.
 */
public class PubsubConnectorTest {
    @Test
    public void testLoadNullConfig() {
        PubsubConnector connector = new PubsubConnector();

        try {
            connector.initialize(null, null);
            fail();
        } catch (Exception ex) {
            assertEquals("configuration cannot be null", ex.getMessage());
        }
    }

    @Test
    public void testLoadCorrectConfig() throws IOException {
        String projectId = "pulsar-io-google-pubsub";
        String topicId = "test-pubsub-topic-" + System.currentTimeMillis();
        String endpoint = "localhost:8085";
        String credential = "";
        String schemaId = "test-pubsub-schema-" + System.currentTimeMillis();
        String schemaType = "AVRO";
        String schemaEncoding = "JSON";
        String schemaDefinition = "{\n"
                + " \"type\" : \"record\",\n"
                + " \"name\" : \"Avro\",\n"
                + " \"fields\" : [\n"
                + "   {\n"
                + "     \"name\" : \"key\",\n"
                + "     \"type\" : \"string\"\n"
                + "   }\n"
                + " ]\n"
                + "}";

        Map<String, Object> properties = new HashMap<>();
        properties.put("pubsubEndpoint", endpoint);
        properties.put("pubsubProjectId", projectId);
        properties.put("pubsubCredential", credential);
        properties.put("pubsubTopicId", topicId);
        properties.put("pubsubSchemaId", schemaId);
        properties.put("pubsubSchemaType", schemaType);
        properties.put("pubsubSchemaEncoding", schemaEncoding);
        properties.put("pubsubSchemaDefinition", schemaDefinition);

        PubsubConnector connector = new PubsubConnector();
        connector.initialize(properties, null);
        if (!(connector.getConfig().getPubsubEndpoint().equals(endpoint)
                || connector.getConfig().getPubsubEndpoint().equals(PubsubUtils.PUBSUB_EMULATOR_HOST))) {
            fail("unable to get the correct PubSub endpoint");
        }
        assertEquals(projectId, connector.getConfig().getPubsubProjectId());
        assertEquals(credential, connector.getConfig().getPubsubCredential());
        assertEquals(topicId, connector.getConfig().getPubsubTopicId());
        assertEquals(schemaId, connector.getConfig().getPubsubSchemaId());
        assertEquals(schemaType, connector.getConfig().getPubsubSchemaType().toString());
        assertEquals(schemaEncoding, connector.getConfig().getPubsubSchemaEncoding().toString());
        assertEquals(schemaDefinition, connector.getConfig().getPubsubSchemaDefinition());
    }
}
