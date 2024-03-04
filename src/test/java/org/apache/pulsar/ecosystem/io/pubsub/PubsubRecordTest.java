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

import static org.apache.pulsar.ecosystem.io.pubsub.PubsubRecord.PULSAR_MESSAGE_KEY;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.functions.api.Record;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit test for {@link PubsubRecord}.
 */
public class PubsubRecordTest {
    @Test
    public void testRecord() {
        String destination = "test-destination";
        String key = "pulsar-io-google-pubsub";
        byte[] value = "hello pulsar".getBytes(StandardCharsets.UTF_8);
        Long publishTime = System.currentTimeMillis();
        Map<String, String> attributesMap = new HashMap<>();
        attributesMap.put(PULSAR_MESSAGE_KEY, key);
        attributesMap.put("apps", "app_a");

        PubsubMessage mockPubsubMessage = Mockito.mock(PubsubMessage.class);

        ByteString mockData = Mockito.mock(ByteString.class);
        Mockito.when(mockData.toByteArray()).thenReturn(value);
        Mockito.when(mockPubsubMessage.getData()).thenReturn(mockData);

        Mockito.when(mockPubsubMessage.getAttributesMap()).thenReturn(attributesMap);

        Timestamp mockPublishTime = Mockito.mock(Timestamp.class);
        Mockito.when(mockPublishTime.getSeconds()).thenReturn(publishTime);
        Mockito.when(mockPubsubMessage.getPublishTime()).thenReturn(mockPublishTime);

        AckReplyConsumer mockAckReplyConsumer = Mockito.mock(AckReplyConsumer.class);

        Record<byte[]> record = new PubsubRecord(destination, mockPubsubMessage, mockAckReplyConsumer);

        Assert.assertEquals(value, record.getValue());

        if (record.getDestinationTopic().isPresent()) {
            Assert.assertEquals(destination, record.getDestinationTopic().get());
        } else {
            Assert.fail("record.getDestinationTopic().isPresent() should return true");
        }
    }
}

