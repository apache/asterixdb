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
package org.apache.asterix.common.storage;

import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

import com.fasterxml.jackson.databind.JsonNode;

public class SizeBoundedConcurrentMergePolicyFactory implements ILSMMergePolicyFactory {

    private static final long serialVersionUID = 1L;
    public static final String NAME = "size-bounded-concurrent";
    public static final String MIN_MERGE_COMPONENT_COUNT = "min-merge-component-count";
    public static final String MAX_MERGE_COMPONENT_COUNT = "max-merge-component-count";
    public static final String MAX_COMPONENT_COUNT = "max-component-count";
    public static final String SIZE_RATIO = "size-ratio";
    public static final Set<String> PROPERTIES_NAMES =
            Set.of(MIN_MERGE_COMPONENT_COUNT, MAX_MERGE_COMPONENT_COUNT, SIZE_RATIO, MAX_COMPONENT_COUNT);

    @Override
    public ILSMMergePolicy createMergePolicy(Map<String, String> configuration, INCServiceContext ctx) {
        long maxComponentSize = ((INcApplicationContext) ctx.getApplicationContext()).getStorageProperties()
                .getStorageMaxComponentSize();
        ILSMMergePolicy policy = new SizeBoundedConcurrentMergePolicy(maxComponentSize);
        policy.configure(configuration);
        return policy;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Set<String> getPropertiesNames() {
        return PROPERTIES_NAMES;
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        return registry.getClassIdentifier(getClass(), serialVersionUID);
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        return new SizeBoundedConcurrentMergePolicyFactory();
    }
}
