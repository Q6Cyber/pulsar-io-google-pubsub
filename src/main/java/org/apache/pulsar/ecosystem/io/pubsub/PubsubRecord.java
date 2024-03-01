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
import com.google.pubsub.v1.PubsubMessage;
import java.util.Map;
import java.util.Optional;
import lombok.Data;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Record;

/**
 * PubsubRecord implements Record interface provided by Pulsar.
 */
@Data
public class PubsubRecord implements Record<byte[]> {
    private final String destination;
    private final PubsubMessage originalMessage;
    public static final String PULSAR_MESSAGE_KEY = "pulsar.key";
    private final AckReplyConsumer ackReplyConsumer;

    public PubsubRecord(String destination, PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
        this.destination = destination;
        this.originalMessage = pubsubMessage;
        this.ackReplyConsumer = ackReplyConsumer;
    }

    public Optional<String> getKey() {
        if (this.originalMessage.getAttributesMap().containsKey(PULSAR_MESSAGE_KEY)) {
            return Optional.of(this.originalMessage.getAttributesMap().get(PULSAR_MESSAGE_KEY));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public byte[] getValue() {
        return this.originalMessage.getData().toByteArray();
    }

    @Override
    public Optional<Long> getEventTime() {
        return Optional.of(this.originalMessage.getPublishTime().getSeconds());
    }

    @Override
    public Map<String, String> getProperties() {
        return this.originalMessage.getAttributesMap();
    }

    @Override
    public Optional<String> getDestinationTopic() {
        return Optional.of(this.destination);
    }

    @Override
    public Schema<byte[]> getSchema() {
        return Schema.BYTES;
    }
}
