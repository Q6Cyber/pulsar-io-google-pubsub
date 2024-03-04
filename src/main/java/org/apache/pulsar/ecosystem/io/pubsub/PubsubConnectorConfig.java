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

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.apache.pulsar.ecosystem.io.pubsub.PubsubUtils.PUBSUB_EMULATOR_HOST;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.SchemaServiceClient;
import com.google.cloud.pubsub.v1.SchemaServiceSettings;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.stub.PublisherStubSettings;
import com.google.pubsub.v1.Encoding;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Schema;
import com.google.pubsub.v1.SchemaName;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.BaseContext;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * PubsubConnectorConfig holds configuration from configuration file.
 * <p>
 * Reference:
 * - https://cloud.google.com/pubsub/docs/admin
 * - https://cloud.google.com/pubsub/docs/emulator#pubsub-emulator-java
 * </p>
 */
@Data
@Slf4j
public class PubsubConnectorConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    @FieldDoc(required = false,
            defaultValue = "",
            help = "pubsubEndpoint is Google Cloud Pub/Sub end-point")
    private String pubsubEndpoint = "";

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "pubsubProjectId is Google Cloud project id"
    )
    private String pubsubProjectId = "";

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "pubsubCredential is Google Cloud credential string, recommend "
                    + "set the GOOGLE_APPLICATION_CREDENTIALS environment variable for authentication, see "
                    + "https://cloud.google.com/docs/authentication/getting-started#setting_the_environment_variable"
    )
    private String pubsubCredential = "";

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "Pulsar secret name to pull the pubsubCredential json from"
    )
    private String pulsarSecretName = "";

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "pubsubTopicId is used to read from or written to Google Cloud Pub/Sub"
    )
    private String pubsubTopicId = "";

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "pubsubSchemaId is used to set schema id on create schema"
    )
    private String pubsubSchemaId = "";

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "pubsubSchemaType is used to set schema type on create schema, only supports: AVRO"
    )
    private Schema.Type pubsubSchemaType = null;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "pubsubSchemaEncoding is used to set schema encoding on create schema, only supports: JSON"
    )
    private Encoding pubsubSchemaEncoding = null;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "pubsubSchemaDefinition is used to create scheme or parse the message on create schema"
    )
    private String pubsubSchemaDefinition = "";

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "pubsubPublisherBatchIsEnabled is to define whether the publisher batch mode is enabled"
    )
    private Boolean pubsubPublisherBatchIsEnabled = null;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "pubsubPublisherBatchDelayThresholdMillis is to define the publisher batch delay threshold millies"
    )
    private Long pubsubPublisherBatchDelayThresholdMillis = null;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "pubsubPublisherBatchElementCountThreshold is to define the publisher batch element count threshold"
    )
    private Long pubsubPublisherBatchElementCountThreshold = null;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "pubsubPublisherBatchElementCountThreshold is to define the publisher batch request byte threshold"
    )
    private Long pubsubPublisherBatchRequestByteThreshold = null;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "pubsubPublisherBatchFlowControlMaxOutstandingElementCount is to define the publisher batch flow control max outstanding element count"
    )
    private Long pubsubPublisherBatchFlowControlMaxOutstandingElementCount = null;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "pubsubPublisherBatchFlowControlMaxOutstandingRequestBytes is to define the publisher batch flow control max outstanding request bytes"
    )
    private Long pubsubPublisherBatchFlowControlMaxOutstandingRequestBytes = null;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "pubsubPublisherBatchFlowControlLimitExceededBehavior is to define the publisher batch flow control limit exceeded behavior"
    )
    private String pubsubPublisherBatchFlowControlLimitExceededBehavior = null;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "pubsubConsumerBatchFlowControlMaxOutstandingElementCount is to define the consumer batch flow control max outstanding element count"
    )
    private Long pubsubConsumerBatchFlowControlMaxOutstandingElementCount = null;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "pubsubConsumerBatchFlowControlMaxOutstandingRequestBytes is to define the consumer batch flow control max outstanding request bytes"
    )
    private Long pubsubConsumerBatchFlowControlMaxOutstandingRequestBytes = null;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "pubsubConsumerBatchFlowControlLimitExceededBehavior is to define the consumer batch flow control limit exceeded behavior"
    )
    private String pubsubConsumerBatchFlowControlLimitExceededBehavior = null;

    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "pubsubConsumerParallelPullCount is to define the consumer parallel pull count"
    )
    private Integer pubsubConsumerParallelPullCount = null;

    private transient TransportChannelProvider transportChannelProvider = null;
    private transient CredentialsProvider credentialsProvider = null;

    public TransportChannelProvider getTransportChannelProvider() {
        return this.transportChannelProvider;
    }

    public static PubsubConnectorConfig load(Map<String, Object> config, BaseContext context) throws IOException {
        if (PubsubUtils.isEmulator()) {
            log.warn("currently connected endpoint is an emulator, if not so, please unset the PUBSUB_EMULATOR_HOST "
                    + "from environment variables");
        }
        ObjectMapper mapper = new ObjectMapper();
        PubsubConnectorConfig pubsubConnectorConfig = mapper.readValue(new ObjectMapper().writeValueAsString(config),
                PubsubConnectorConfig.class);
        pubsubConnectorConfig.transportChannelProvider = pubsubConnectorConfig.newTransportChannelProvider();
        pubsubConnectorConfig.credentialsProvider = pubsubConnectorConfig.newCredentialsProvider(context);
        return pubsubConnectorConfig;
    }

    private CredentialsProvider newCredentialsProvider(BaseContext context) throws IOException {
        if (!isNullOrEmpty(this.pulsarSecretName) && context != null) {
            String secret = context.getSecret(this.pulsarSecretName);

            if (isNullOrEmpty(secret)) {
                throw new IllegalArgumentException("If pulsarSecretName is specified, the pulsar secret must not be empty.");
            }

            this.pubsubCredential = secret;
        }

        if (!isNullOrEmpty(PUBSUB_EMULATOR_HOST)) {
            return NoCredentialsProvider.create();
        }

        if (isNullOrEmpty(this.pubsubCredential)) {
            return PublisherStubSettings.defaultCredentialsProviderBuilder().build();
        }

        return FixedCredentialsProvider.create(GoogleCredentials
                .fromStream(new ByteArrayInputStream(this.pubsubCredential.getBytes(StandardCharsets.UTF_8))));
    }

    public void validate() {
        if (pubsubProjectId == null || pubsubProjectId.equals("")) {
            throw new IllegalArgumentException("pubsubProjectId is required");
        }

        if (pubsubTopicId == null || pubsubTopicId.equals("")) {
            throw new IllegalArgumentException("pubsubTopicId is required");
        }

        if (pubsubSchemaId != null && !"".equals(pubsubSchemaId)) {
            if (pubsubSchemaType == null) {
                throw new IllegalArgumentException("pubsubSchemaType cannot be null, when pubsubSchemaId is set");
            } else {
                if (pubsubSchemaType != Schema.Type.AVRO) {
                    throw new IllegalArgumentException("pubsubSchemaType only supports AVRO");
                }
            }
            if (pubsubSchemaEncoding == null) {
                throw new IllegalArgumentException("pubsubSchemaEncoding cannot be null, when pubsubSchemaId is set");
            } else {
                if (pubsubSchemaEncoding != Encoding.JSON) {
                    throw new IllegalArgumentException("pubsubSchemaEncoding only supports JSON");
                }
            }
            if (pubsubSchemaDefinition == null) {
                throw new IllegalArgumentException("pubsubSchemaDefinition cannot be null, when pubsubSchemaId is set");
            }
        }
    }

    private TransportChannelProvider newTransportChannelProvider() {
        if (PubsubUtils.isEmulator()) {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(PUBSUB_EMULATOR_HOST).usePlaintext().build();
            return FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
        }

        return PublisherStubSettings.defaultTransportChannelProvider();
    }


    public String getPubsubEndpoint() {
        return PubsubUtils.toEndpoint(pubsubEndpoint);
    }

    public Subscriber newSubscriber(MessageReceiver receiver) throws IOException {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(pubsubProjectId, pubsubTopicId);

        SubscriptionAdminSettings subscriptionAdminSettings =
                SubscriptionAdminSettings.newBuilder()
                        .setTransportChannelProvider(this.transportChannelProvider)
                        .setCredentialsProvider(this.credentialsProvider)
                        .build();
        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient
                .create(subscriptionAdminSettings)) {
            TopicName topicName = TopicName.of(pubsubProjectId, pubsubTopicId);

            try {
                subscriptionAdminClient.getSubscription(subscriptionName);
            } catch (NotFoundException ex) {
                // when the subscription does not exist, it need to be created.
                while (true) {
                    Exception exception = null;
                    try {
                        subscriptionAdminClient
                                .createSubscription(subscriptionName, topicName, PushConfig.getDefaultInstance(), 10);
                        log.info("{} subscription created successfully", subscriptionName);
                    } catch (Exception e) {
                        exception = e;
                        log.error("failed to create subscription", ex);
                    }

                    if (exception == null || exception instanceof AlreadyExistsException) {
                        break;
                    }
                }
            }
        }

        Subscriber.Builder subscriberBuilder = Subscriber.newBuilder(subscriptionName, receiver)
                .setEndpoint(PubsubUtils.toEndpoint(this.pubsubEndpoint))
                .setChannelProvider(this.transportChannelProvider)
                .setCredentialsProvider(this.credentialsProvider);
        if (Stream.of(this.pubsubConsumerBatchFlowControlMaxOutstandingElementCount,
                this.pubsubConsumerBatchFlowControlMaxOutstandingRequestBytes,
                this.pubsubConsumerBatchFlowControlLimitExceededBehavior).anyMatch(Objects::nonNull)) {
            FlowControlSettings.Builder flowBuilder = FlowControlSettings.newBuilder();
            Optional.ofNullable(this.pubsubConsumerBatchFlowControlMaxOutstandingElementCount)
                    .ifPresent(flowBuilder::setMaxOutstandingElementCount);
            Optional.ofNullable(this.pubsubConsumerBatchFlowControlMaxOutstandingRequestBytes)
                    .ifPresent(flowBuilder::setMaxOutstandingRequestBytes);
            Optional.ofNullable(this.pubsubConsumerBatchFlowControlLimitExceededBehavior)
                    .map(FlowController.LimitExceededBehavior::valueOf)
                    .ifPresent(flowBuilder::setLimitExceededBehavior);
            subscriberBuilder.setFlowControlSettings(flowBuilder.build());
        }
        Optional.ofNullable(this.pubsubConsumerParallelPullCount)
                .ifPresent(subscriberBuilder::setParallelPullCount);

        Subscriber subscriber = subscriberBuilder.build();
        subscriber.startAsync().awaitRunning();

        return subscriber;
    }

    public Schema getOrCreateSchema() throws IOException {
        SchemaName schemaName = SchemaName.of(this.pubsubProjectId, this.pubsubSchemaId);
        SchemaServiceSettings schemaServiceSettings = SchemaServiceSettings.newBuilder()
                .setTransportChannelProvider(this.transportChannelProvider)
                .setCredentialsProvider(this.credentialsProvider)
                .build();
        Schema schema;
        try (SchemaServiceClient schemaServiceClient = SchemaServiceClient.create(schemaServiceSettings)) {
            try {
                schema = schemaServiceClient.getSchema(schemaName);
            } catch (Exception ex) {
                if (!(ex instanceof NotFoundException)) {
                    throw ex;
                }
                schema = schemaServiceClient.createSchema(
                        ProjectName.of(this.pubsubProjectId),
                        Schema.newBuilder()
                                .setType(this.pubsubSchemaType)
                                .setDefinition(this.pubsubSchemaDefinition)
                                .build(),
                        this.pubsubSchemaId);
            }
        }
        return schema;
    }
}
