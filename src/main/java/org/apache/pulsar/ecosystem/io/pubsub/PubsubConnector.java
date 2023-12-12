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

import java.io.IOException;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.BaseContext;

/**
 * PubsubConnector is base class for sink and source.
 */
@Slf4j
public class PubsubConnector {
    @Getter
    private PubsubConnectorConfig config;

    public void initialize(Map<String, Object> config, BaseContext context) throws IOException {
        if (config == null) {
            throw new IllegalArgumentException("configuration cannot be null");
        }

        // load the configuration and validate it
        this.config = PubsubConnectorConfig.load(config, context);
        this.config.validate();
    }
}
