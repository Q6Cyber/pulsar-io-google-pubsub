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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController.LimitExceededBehavior;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.pubsub.v1.Encoding;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Schema;
import com.google.pubsub.v1.SchemaSettings;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.ecosystem.io.pubsub.util.AvroUtils;
import org.apache.pulsar.ecosystem.io.pubsub.util.ProtobufUtils;
import org.apache.pulsar.functions.api.Record;
import org.threeten.bp.Duration;


/**
 * PubsubPublisher wrapper Publisher of Google Cloud Pub/Sub.
 * <p>
 * Reference:
 * - https://cloud.google.com/pubsub/docs/samples/pubsub-create-avro-schema
 * - https://cloud.google.com/pubsub/docs/samples/pubsub-publish-avro-records#pubsub_publish_avro_records-java
 * - https://cloud.google.com/pubsub/docs/schemas#gcloud
 * </p>
 */
@Slf4j
public class PubsubPublisher {
    private final Publisher publisher;
    private final Topic topic;
    private final Object messageSchema;
    private final Schema.Type schemaType;

    private PubsubPublisher(Publisher publisher,
                            Topic topic,
                            Schema.Type schemaType, Object messageSchema) {
        this.publisher = publisher;
        this.topic = topic;
        this.schemaType = schemaType;
        this.messageSchema = messageSchema;
    }

    public static PubsubPublisher create(PubsubConnectorConfig config) throws Exception {
        TopicName topicName = TopicName.of(config.getPubsubProjectId(), config.getPubsubTopicId());

        TopicAdminSettings topicAdminSettings = TopicAdminSettings.newBuilder()
                .setTransportChannelProvider(config.getTransportChannelProvider())
                .setCredentialsProvider(config.getCredentialsProvider())
                .build();

        Topic topic;
        Schema schema = null;
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create(topicAdminSettings)) {
            try {
                topic = topicAdminClient.getTopic(topicName);
                if (!"".equals(config.getPubsubSchemaId())) {
                    schema = config.getOrCreateSchema();
                }
            } catch (Exception ex) {
                if (ex instanceof NotFoundException) {
                    Topic.Builder topicBuilder = Topic.newBuilder();
                    topicBuilder.setName(topicName.toString());

                    if (!"".equals(config.getPubsubSchemaId())) {
                        schema = config.getOrCreateSchema();
                        SchemaSettings schemaSettings = SchemaSettings.newBuilder()
                                .setSchema(schema.getName())
                                .setEncoding(config.getPubsubSchemaEncoding())
                                .build();
                        topicBuilder.setSchemaSettings(schemaSettings);
                    }

                    topic = topicAdminClient.createTopic(topicBuilder.build());
                    log.info("{} topic created successfully", topicName);
                } else {
                    log.error("failed to create topic", ex);
                    throw ex;
                }
            }
        }

        String formattedSchema = topic.getSchemaSettings().getSchema();
        Object messageSchema = null;
        Schema.Type schemaType = null;

        if (!formattedSchema.contains("_deleted-schema_") && !"".equals(formattedSchema)) {
            if (schema == null) {
                throw new Exception("schema cannot be null, should be " + formattedSchema);
            }
            schemaType = schema.getType();
            if (schemaType == Schema.Type.AVRO) {
                messageSchema = AvroUtils.parseSchemaString(schema.getDefinition());
            } else if (schemaType == Schema.Type.PROTOCOL_BUFFER) {
                messageSchema = ProtobufUtils.parseSchemaString(schema.getDefinition());
            } else {
                throw new Exception("not supported scheme type " + schemaType);
            }
        }

        Publisher.Builder publishBuilder = Publisher.newBuilder(topicName)
                .setEndpoint(PubsubUtils.toEndpoint(config.getPubsubEndpoint()))
                .setChannelProvider(config.getTransportChannelProvider())
                .setCredentialsProvider(config.getCredentialsProvider())
                .setBatchingSettings(buildBatchSettings(config));

        return new PubsubPublisher(publishBuilder.build(), topic, schemaType, messageSchema);
    }

    private static BatchingSettings buildBatchSettings(PubsubConnectorConfig config) {
        BatchingSettings.Builder batchingSettings = BatchingSettings.newBuilder();

        Optional.ofNullable(config.getPubsubPublisherBatchIsEnabled())
            .ifPresent(batchingSettings::setIsEnabled);
        Optional.ofNullable(config.getPubsubPublisherBatchDelayThresholdMillis())
            .map(Duration::ofMillis)
            .ifPresent(batchingSettings::setDelayThreshold);
        Optional.ofNullable(config.getPubsubPublisherBatchElementCountThreshold())
            .ifPresent(batchingSettings::setElementCountThreshold);
        Optional.ofNullable(config.getPubsubPublisherBatchRequestByteThreshold())
            .ifPresent(batchingSettings::setRequestByteThreshold);

        buildBatchFlowControlSettings(config).ifPresent(batchingSettings::setFlowControlSettings);

        return batchingSettings.build();
    }

    private static Optional<FlowControlSettings> buildBatchFlowControlSettings(PubsubConnectorConfig config) {
        if (
            Stream.of(
                config.getPubsubPublisherBatchFlowControlLimitExceededBehavior(),
                config.getPubsubPublisherBatchFlowControlMaxOutstandingElementCount(),
                config.getPubsubPublisherBatchFlowControlMaxOutstandingRequestBytes()
            )
            .noneMatch(Objects::nonNull)
        ) {
            return Optional.empty();
        }

        FlowControlSettings.Builder flowControlSettings = FlowControlSettings.newBuilder();

        Optional.ofNullable(config.getPubsubPublisherBatchFlowControlLimitExceededBehavior())
            .map(LimitExceededBehavior::valueOf)
            .ifPresent(flowControlSettings::setLimitExceededBehavior);
        Optional.ofNullable(config.getPubsubPublisherBatchFlowControlMaxOutstandingElementCount())
            .ifPresent(flowControlSettings::setMaxOutstandingElementCount);
        Optional.ofNullable(config.getPubsubPublisherBatchFlowControlMaxOutstandingRequestBytes())
            .ifPresent(flowControlSettings::setMaxOutstandingRequestBytes);

        return Optional.of(flowControlSettings.build());
    }

    public void send(Record<GenericObject> record, ApiFutureCallback<String> callback) throws Exception {
        ByteString data = recordToByteString(record);
        if (data == null) {
            log.warn("skip the empty record {}", record);
            callback.onSuccess(null);
            return;
        }
        Map<String, String> attributes = new HashMap<>();
        Optional.ofNullable(record.getProperties())
            .ifPresent(attributes::putAll);
        record.getMessage()
                .map(Message::getProperties)
                .map(Map::entrySet)
                .stream()
                .flatMap(Collection::stream)
                .filter(entry -> !attributes.containsKey(entry.getKey()))
                .forEach(entry -> attributes.put(entry.getKey(), String.valueOf(entry.getValue())));

        PubsubMessage message = PubsubMessage.newBuilder().setData(data).putAllAttributes(attributes).build();
        ApiFuture<String> apiFuture = publisher.publish(message);
        if (callback != null) {
            ApiFutures.addCallback(apiFuture, callback, MoreExecutors.directExecutor());
        }
    }

    private boolean hasSchema() {
        return this.schemaType != null && this.messageSchema != null;
    }

    public ByteString serializeAvroSchema(GenericRecord record) throws IOException {
        Encoding encoding = this.topic.getSchemaSettings().getEncoding();
        switch (schemaType) {
            case AVRO:
                switch (encoding) {
                    case BINARY:
                        return ByteString.copyFrom(serializeAvroWithBinaryEncoder(record,
                                (org.apache.avro.Schema) this.messageSchema));
                    case JSON:
                        return ByteString.copyFrom(serializeAvroJsonEncoder(record,
                                (org.apache.avro.Schema) this.messageSchema));
                    default:
                        throw new RuntimeException("not support encoding type: " + encoding);
                }
            default:
                throw new RuntimeException("not support encoding type: " + encoding);
        }
    }

    public static byte[] serializeAvroWithBinaryEncoder(GenericRecord record, org.apache.avro.Schema schema)
            throws IOException {
        SpecificDatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
        datumWriter.write(record, binaryEncoder);
        binaryEncoder.flush();
        return byteArrayOutputStream.toByteArray();
    }

    public static byte[] serializeAvroJsonEncoder(GenericRecord record, org.apache.avro.Schema schema)
            throws IOException {
        SpecificDatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, byteArrayOutputStream);
        datumWriter.write(record, jsonEncoder);
        jsonEncoder.flush();
        return byteArrayOutputStream.toByteArray();
    }

    public static GenericRecord deserializeAvroWithBinaryEncoder(byte[] recordBytes, org.apache.avro.Schema schema)
            throws IOException {
        DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(schema);
        ByteArrayInputStream stream = new ByteArrayInputStream(recordBytes);
        BinaryDecoder binaryDecoder = new DecoderFactory().binaryDecoder(stream, null);
        return datumReader.read(null, binaryDecoder);
    }

    public static GenericRecord deserializeAvroWithJsonEncoder(byte[] recordBytes, org.apache.avro.Schema schema)
            throws IOException {
        DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(schema);
        ByteArrayInputStream stream = new ByteArrayInputStream(recordBytes);
        JsonDecoder jsonDecoder = new DecoderFactory().jsonDecoder(schema, stream);
        return datumReader.read(null, jsonDecoder);
    }

    ByteString recordToByteString(Record<GenericObject> record)
            throws IOException {
        if (record.getSchema() == null) {
            if (record.getMessage().isPresent()) {
                return ByteString.copyFrom(record.getMessage().get().getData());
            } else {
                return null;
            }
        }

        switch (record.getValue().getSchemaType()) {
            case PROTOBUF:
            case PROTOBUF_NATIVE:
                if (hasSchema()) {
                    throw new RuntimeException("not support convert data of PROTOBUF/PROTOBUF schema type");
                }
                DynamicMessage dynamicMessage = (DynamicMessage) record.getValue().getNativeObject();
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                dynamicMessage.writeTo(out);
                return ByteString.copyFrom(out.toByteArray());
            case BYTES:
                return ByteString.copyFrom((byte[]) record.getValue().getNativeObject());
            case AVRO:
                GenericRecord genericRecord = (GenericRecord) record.getValue().getNativeObject();
                if (hasSchema()) {
                    return serializeAvroSchema(genericRecord);
                }
                return ByteString.copyFromUtf8(String.valueOf(record.getValue().getNativeObject()));
            default:
                String data = String.valueOf(record.getValue().getNativeObject());
                return ByteString.copyFromUtf8(data);
        }
    }

    public void shutdown() throws InterruptedException {
        this.publisher.awaitTermination(3, TimeUnit.SECONDS);
        this.publisher.shutdown();
    }

    public Publisher getPublisher() {
        return this.publisher;
    }
}
