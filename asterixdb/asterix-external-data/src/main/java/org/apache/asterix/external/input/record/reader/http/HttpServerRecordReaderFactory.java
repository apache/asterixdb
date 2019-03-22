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
package org.apache.asterix.external.input.record.reader.http;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.http.server.HttpServerConfigBuilder;

public class HttpServerRecordReaderFactory implements IRecordReaderFactory<char[]> {

    private static final String KEY_CONFIGURATION_ADDRESSES = "addresses";
    private static final String KEY_CONFIGURATION_PATH = "path";
    private static final String KEY_CONFIGURATION_QUEUE_SIZE = "queue_size";

    private static final List<String> recordReaderNames =
            Collections.singletonList(ExternalDataConstants.KEY_ADAPTER_NAME_HTTP);

    private String entryPoint;
    private String addrValue;
    private int queueSize;
    private Map<String, String> configurations;
    private List<Pair<String, Integer>> serverAddrs;

    @Override
    public IRecordReader<? extends char[]> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        try {
            return new HttpServerRecordReader(serverAddrs.get(partition).getRight(), entryPoint, queueSize,
                    HttpServerConfigBuilder.createDefault());
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public Class<?> getRecordClass() {
        return char[].class;
    }

    @Override
    public List<String> getRecordReaderNames() {
        return recordReaderNames;
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() {
        return FeedUtils.addressToAbsolutePartitionConstraints(serverAddrs);
    }

    private String getConfigurationValue(String key, boolean required) throws CompilationException {
        String value = configurations.get(key);
        if (value == null && required) {
            throw new CompilationException("Required configuration missing: " + key);
        }
        return value;
    }

    @Override
    public void configure(IServiceContext ctx, Map<String, String> configuration) throws AlgebricksException {
        this.configurations = configuration;
        // necessary configs
        addrValue = getConfigurationValue(KEY_CONFIGURATION_ADDRESSES, true);
        serverAddrs = FeedUtils.extractHostsPorts(getConfigurationValue(ExternalDataConstants.KEY_MODE, true), ctx,
                addrValue);
        // optional configs
        String queueSizeStr = getConfigurationValue(KEY_CONFIGURATION_QUEUE_SIZE, false);
        queueSize = queueSizeStr == null ? 0 : Integer.valueOf(queueSizeStr);
        entryPoint = getConfigurationValue(KEY_CONFIGURATION_PATH, false);
    }
}
