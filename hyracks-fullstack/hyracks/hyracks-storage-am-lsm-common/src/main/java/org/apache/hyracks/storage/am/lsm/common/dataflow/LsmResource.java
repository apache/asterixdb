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
package org.apache.hyracks.storage.am.lsm.common.dataflow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpPageWriteCallbackFactory;
import org.apache.hyracks.storage.common.IResource;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.LocalResource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The base resource that will be written to disk. it will go in the serializable resource
 * member in {@link LocalResource}
 */
public abstract class LsmResource implements IResource {

    private static final long serialVersionUID = 1L;
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    protected String path;
    protected final IStorageManager storageManager;
    protected final ITypeTraits[] typeTraits;
    protected final IBinaryComparatorFactory[] cmpFactories;
    protected final ITypeTraits[] filterTypeTraits;
    protected final IBinaryComparatorFactory[] filterCmpFactories;
    protected final int[] filterFields;
    protected final ILSMOperationTrackerFactory opTrackerProvider;
    protected final ILSMIOOperationCallbackFactory ioOpCallbackFactory;
    protected final ILSMPageWriteCallbackFactory pageWriteCallbackFactory;
    protected final IMetadataPageManagerFactory metadataPageManagerFactory;
    protected final IVirtualBufferCacheProvider vbcProvider;
    protected final ILSMIOOperationSchedulerProvider ioSchedulerProvider;
    protected final ILSMMergePolicyFactory mergePolicyFactory;
    protected final Map<String, String> mergePolicyProperties;
    protected final boolean durable;

    public LsmResource(String path, IStorageManager storageManager, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields,
            ILSMOperationTrackerFactory opTrackerProvider, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IMetadataPageManagerFactory metadataPageManagerFactory, IVirtualBufferCacheProvider vbcProvider,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, boolean durable) {
        this.path = path;
        this.storageManager = storageManager;
        this.typeTraits = typeTraits;
        this.cmpFactories = cmpFactories;
        this.filterTypeTraits = filterTypeTraits;
        this.filterCmpFactories = filterCmpFactories;
        this.filterFields = filterFields;
        this.opTrackerProvider = opTrackerProvider;
        this.ioOpCallbackFactory = ioOpCallbackFactory;
        this.pageWriteCallbackFactory = pageWriteCallbackFactory;
        this.metadataPageManagerFactory = metadataPageManagerFactory;
        this.vbcProvider = vbcProvider;
        this.ioSchedulerProvider = ioSchedulerProvider;
        this.mergePolicyFactory = mergePolicyFactory;
        this.mergePolicyProperties = mergePolicyProperties;
        this.durable = durable;
    }

    protected LsmResource(IPersistedResourceRegistry registry, JsonNode json) throws HyracksDataException {
        path = json.get("path").asText();
        storageManager = (IStorageManager) registry.deserialize(json.get("storageManager"));
        final List<ITypeTraits> typeTraitsList = new ArrayList<>();
        final ArrayNode jsonTypeTraits = (ArrayNode) json.get("typeTraits");
        for (JsonNode tt : jsonTypeTraits) {
            typeTraitsList.add((ITypeTraits) registry.deserialize(tt));
        }
        typeTraits = typeTraitsList.toArray(new ITypeTraits[0]);

        final List<IBinaryComparatorFactory> cmpFactoriesList = new ArrayList<>();
        final ArrayNode jsonCmpFactories = (ArrayNode) json.get("cmpFactories");
        for (JsonNode cf : jsonCmpFactories) {
            cmpFactoriesList.add((IBinaryComparatorFactory) registry.deserialize(cf));
        }
        cmpFactories = cmpFactoriesList.toArray(new IBinaryComparatorFactory[0]);

        if (json.hasNonNull("filterTypeTraits")) {
            final List<ITypeTraits> filterTypeTraitsList = new ArrayList<>();
            final ArrayNode jsonFilterTypeTraits = (ArrayNode) json.get("filterTypeTraits");
            for (JsonNode tt : jsonFilterTypeTraits) {
                filterTypeTraitsList.add((ITypeTraits) registry.deserialize(tt));
            }
            filterTypeTraits = filterTypeTraitsList.toArray(new ITypeTraits[0]);
        } else {
            filterTypeTraits = null;
        }

        if (json.hasNonNull("filterCmpFactories")) {
            final List<IBinaryComparatorFactory> filterCmpFactoriesList = new ArrayList<>();
            final ArrayNode jsonFilterCmpFactories = (ArrayNode) json.get("filterCmpFactories");
            for (JsonNode cf : jsonFilterCmpFactories) {
                filterCmpFactoriesList.add((IBinaryComparatorFactory) registry.deserialize(cf));
            }
            filterCmpFactories = filterCmpFactoriesList.toArray(new IBinaryComparatorFactory[0]);
        } else {
            filterCmpFactories = null;
        }

        filterFields = OBJECT_MAPPER.convertValue(json.get("filterFields"), int[].class);
        opTrackerProvider = (ILSMOperationTrackerFactory) registry.deserialize(json.get("opTrackerProvider"));
        ioOpCallbackFactory = (ILSMIOOperationCallbackFactory) registry.deserialize(json.get("ioOpCallbackFactory"));
        if (json.has("pageWriteCallbackFactory")) {
            // for backward-compatibility
            pageWriteCallbackFactory =
                    (ILSMPageWriteCallbackFactory) registry.deserialize(json.get("pageWriteCallbackFactory"));
        } else {
            // treat legacy datasets as no op
            pageWriteCallbackFactory = NoOpPageWriteCallbackFactory.INSTANCE;
        }

        metadataPageManagerFactory =
                (IMetadataPageManagerFactory) registry.deserialize(json.get("metadataPageManagerFactory"));
        if (json.hasNonNull("vbcProvider")) {
            vbcProvider = (IVirtualBufferCacheProvider) registry.deserialize(json.get("vbcProvider"));
        } else {
            vbcProvider = null;
        }
        ioSchedulerProvider = (ILSMIOOperationSchedulerProvider) registry.deserialize(json.get("ioSchedulerProvider"));
        mergePolicyFactory = (ILSMMergePolicyFactory) registry.deserialize(json.get("mergePolicyFactory"));
        mergePolicyProperties = OBJECT_MAPPER.convertValue(json.get("mergePolicyProperties"), Map.class);
        durable = json.get("durable").asBoolean();
    }

    protected void appendToJson(final ObjectNode json, IPersistedResourceRegistry registry)
            throws HyracksDataException {
        json.put("path", path);
        json.set("storageManager", storageManager.toJson(registry));
        ArrayNode ttArray = OBJECT_MAPPER.createArrayNode();
        for (ITypeTraits tt : typeTraits) {
            ttArray.add(tt.toJson(registry));
        }
        json.set("typeTraits", ttArray);

        ArrayNode cmpArray = OBJECT_MAPPER.createArrayNode();
        for (IBinaryComparatorFactory factory : cmpFactories) {
            cmpArray.add(factory.toJson(registry));
        }
        json.set("cmpFactories", cmpArray);

        if (filterTypeTraits != null) {
            ArrayNode fttArray = OBJECT_MAPPER.createArrayNode();
            for (ITypeTraits tt : filterTypeTraits) {
                fttArray.add(tt.toJson(registry));
            }
            json.set("filterTypeTraits", fttArray);
        } else {
            json.set("filterTypeTraits", null);
        }

        if (filterCmpFactories != null) {
            ArrayNode filterCmpArray = OBJECT_MAPPER.createArrayNode();
            for (IBinaryComparatorFactory factory : filterCmpFactories) {
                filterCmpArray.add(factory.toJson(registry));
            }
            json.set("filterCmpFactories", filterCmpArray);
        } else {
            json.set("filterCmpFactories", null);
        }

        json.putPOJO("filterFields", filterFields);
        json.set("opTrackerProvider", opTrackerProvider.toJson(registry));
        json.set("ioOpCallbackFactory", ioOpCallbackFactory.toJson(registry));
        json.set("pageWriteCallbackFactory", pageWriteCallbackFactory.toJson(registry));
        json.set("metadataPageManagerFactory", metadataPageManagerFactory.toJson(registry));
        if (vbcProvider != null) {
            json.set("vbcProvider", vbcProvider.toJson(registry));
        }
        json.set("ioSchedulerProvider", ioSchedulerProvider.toJson(registry));
        json.set("mergePolicyFactory", mergePolicyFactory.toJson(registry));
        json.putPOJO("mergePolicyProperties", mergePolicyProperties);
        json.put("durable", durable);
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public void setPath(String path) {
        this.path = path;
    }
}
