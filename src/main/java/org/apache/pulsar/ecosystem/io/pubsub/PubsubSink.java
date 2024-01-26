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

import com.google.api.core.ApiFutureCallback;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

/**
 * PubsubSink feed data from Pulsar into Google Cloud Pub/Sub.
 */
@Slf4j
public class PubsubSink extends PubsubConnector implements Sink<GenericObject> {
    private SinkContext sinkContext;
    private PubsubPublisher publisher;
    private static final String METRICS_TOTAL_SUCCESS = "_pubsub_sink_total_success_";
    private static final String METRICS_TOTAL_FAILURE = "_pubsub_sink_total_failure_";

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        this.sinkContext = sinkContext;
        initialize(config, sinkContext);
        this.publisher = PubsubPublisher.create(getConfig());
    }

    @Override
    public void write(Record<GenericObject> record) throws Exception {
        this.publisher.send(record, new ApiFutureCallback<String>() {
            @Override
            public void onFailure(Throwable throwable) {
                fail(record);
                log.error("Failed to sink the record", throwable);
            }

            @Override
            public void onSuccess(String s) {
                success(record);
            }
        });
    }

    private void success(Record<GenericObject> record) {
        record.ack();
        if (sinkContext != null) {
            sinkContext.recordMetric(METRICS_TOTAL_SUCCESS, 1);
        }
    }

    private void fail(Record<GenericObject> record) {
        record.fail();
        if (sinkContext != null) {
            sinkContext.recordMetric(METRICS_TOTAL_FAILURE, 1);
        }
    }

    @Override
    public void close() throws InterruptedException {
        if (publisher != null) {
            publisher.shutdown();
        }
    }
}
