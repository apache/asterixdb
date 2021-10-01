/*
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

package org.apache.asterix.jdbc.core;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.module.SimpleModule;

final class ADBDriverContext {

    final Class<? extends ADBDriverBase> driverClass;

    final ADBErrorReporter errorReporter;

    final ADBProductVersion driverVersion;

    final Map<String, ADBDriverProperty> supportedProperties;

    final Logger logger;

    final ObjectReader genericObjectReader;

    final ObjectWriter genericObjectWriter;

    final ObjectReader admFormatObjectReader;

    final ObjectWriter admFormatObjectWriter;

    ADBDriverContext(Class<? extends ADBDriverBase> driverClass,
            Collection<ADBDriverProperty> driverSupportedProperties, ADBErrorReporter errorReporter) {
        this.driverClass = Objects.requireNonNull(driverClass);
        this.errorReporter = Objects.requireNonNull(errorReporter);
        this.logger = Logger.getLogger(driverClass.getName());
        this.driverVersion = ADBProductVersion.parseDriverVersion(driverClass.getPackage());
        this.supportedProperties = createPropertyIndexByName(driverSupportedProperties);

        ObjectMapper genericObjectMapper = ADBProtocol.createObjectMapper();
        this.genericObjectReader = genericObjectMapper.reader();
        this.genericObjectWriter = genericObjectMapper.writer();
        ObjectMapper admFormatObjectMapper = createADMFormatObjectMapper();
        this.admFormatObjectReader = admFormatObjectMapper.reader();
        this.admFormatObjectWriter = admFormatObjectMapper.writer();
    }

    protected ObjectMapper createADMFormatObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule serdeModule = new SimpleModule(driverClass.getName());
        ADBStatement.configureSerialization(serdeModule);
        ADBRowStore.configureDeserialization(mapper, serdeModule);
        mapper.registerModule(serdeModule);
        return mapper;
    }

    private Map<String, ADBDriverProperty> createPropertyIndexByName(Collection<ADBDriverProperty> properties) {
        Map<String, ADBDriverProperty> m = new LinkedHashMap<>();
        for (ADBDriverProperty p : properties) {
            m.put(p.getPropertyName(), p);
        }
        return Collections.unmodifiableMap(m);
    }
}
