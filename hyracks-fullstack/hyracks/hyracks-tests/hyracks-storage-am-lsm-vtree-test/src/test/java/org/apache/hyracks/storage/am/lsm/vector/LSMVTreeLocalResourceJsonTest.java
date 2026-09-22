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
package org.apache.hyracks.storage.am.lsm.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.common.freepage.AppendOnlyLinkedMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.vector.dataflow.LSMVTreeLocalResource;
import org.apache.hyracks.storage.am.vector.TestDoubleArrayVectorAccessor;
import org.apache.hyracks.storage.am.vector.TestVTreeDistanceFunctionFactory;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;
import org.apache.hyracks.storage.am.vector.utils.CrossPollinationConfig;
import org.apache.hyracks.storage.common.IStorageManager;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Round-trips {@link LSMVTreeLocalResource} through {@code toJson}/{@code fromJson}. A local resource is
 * the only description of an index that survives a restart, so a field that fails to round-trip surfaces
 * as a silently mis-configured index rather than as an error here.
 */
public class LSMVTreeLocalResourceJsonTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final int[] VECTOR_FIELDS = new int[] { 0 };
    private static final int[] FILTER_FIELDS = new int[] { 3 };
    private static final int[] IDENTITY_FIELDS = new int[] { 2 };
    private static final int VECTOR_DIMENSIONS = 4;
    private static final int NUM_INCLUDE_FIELDS = 2;
    private static final double EPSILON = 0.75;
    private static final CrossPollinationConfig CROSS_POLLINATION = new CrossPollinationConfig(3, 1.0);

    /** Every key {@code appendToJson} writes unconditionally, so every key {@code fromJson} may not default. */
    private static final String[] REQUIRED_KEYS =
            { "vectorDimensions", "vectorFields", "identityFields", "numIncludeFields", "epsilon", "crossPollinationM",
                    "rngFactor", "vectorAccessorFactory", "distanceFunctionFactory" };

    @Test
    public void quantizedResourceRoundTrips() throws Exception {
        FakeRegistry registry = new FakeRegistry();
        LSMVTreeLocalResource original = resource(registry);
        VTreeQuantizationParams params = new VTreeQuantizationParams(0.1f, 0.9f, 0.8f, 0.999f, 8, 20000);
        original.setQuantizationParameters(params);

        JsonNode json = original.toJson(registry);
        LSMVTreeLocalResource restored = (LSMVTreeLocalResource) LSMVTreeLocalResource.fromJson(registry, json);

        assertEquals(text(json), text(restored.toJson(registry)));
        assertNotNull(restored.getQuantizationParams());
        assertEquals(params, restored.getQuantizationParams());
    }

    /** A truncated or foreign resource must be rejected, not silently defaulted into a mis-keyed index. */
    @Test
    public void missingRequiredKeyIsRejected() throws Exception {
        FakeRegistry registry = new FakeRegistry();
        JsonNode json = resource(registry).toJson(registry);
        for (String key : REQUIRED_KEYS) {
            ObjectNode truncated = json.deepCopy();
            truncated.remove(key);
            try {
                LSMVTreeLocalResource.fromJson(registry, truncated);
                fail("fromJson accepted a resource with no `" + key + "`");
            } catch (HyracksDataException e) {
                assertEquals("expected the message to name the missing key `" + key + "`", true,
                        e.getMessage().contains(key));
            }
        }
    }

    /** The base class owns filterFields; a subclass copy would write it twice and read back the later one. */
    @Test
    public void filterFieldsIsWrittenOnce() throws Exception {
        FakeRegistry registry = new FakeRegistry();
        String json = resource(registry).toJson(registry).toString();
        assertEquals(1, countOccurrences(json, "\"filterFields\""));
    }

    /**
     * @return the serialized form, since {@code putPOJO} wraps an {@code int[]} in a node that compares by
     *         reference and so never equals its deserialized twin.
     */
    private static String text(JsonNode json) {
        return json.toString();
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + needle.length())) {
            count++;
        }
        return count;
    }

    private static LSMVTreeLocalResource resource(FakeRegistry registry) {
        return new LSMVTreeLocalResource("dataverse/dataset/0/idx", registry.serializable(IStorageManager.class),
                new ITypeTraits[] { registry.serializable(ITypeTraits.class) },
                new IBinaryComparatorFactory[] { registry.serializable(IBinaryComparatorFactory.class) }, null, null,
                FILTER_FIELDS, registry.serializable(ILSMOperationTrackerFactory.class),
                NoOpIOOperationCallbackFactory.INSTANCE, NoOpPageWriteCallbackFactory.INSTANCE,
                AppendOnlyLinkedMetadataPageManagerFactory.INSTANCE, null,
                registry.serializable(ILSMIOOperationSchedulerProvider.class),
                registry.serializable(ILSMMergePolicyFactory.class), Collections.emptyMap(), true, VECTOR_DIMENSIONS,
                VECTOR_FIELDS, null, null, true, TestDoubleArrayVectorAccessor.Factory.INSTANCE, IDENTITY_FIELDS,
                NUM_INCLUDE_FIELDS, TestVTreeDistanceFunctionFactory.INSTANCE, CROSS_POLLINATION, EPSILON);
    }

    /**
     * Serializes a collaborator to a token and hands the same instance back for it, so the test exercises
     * the resource's own JSON rather than the registry's class table. Collaborators that write themselves
     * (the {@code NoOp*} singletons) are recognized by the class name they put in their identifier node.
     */
    private static final class FakeRegistry implements IPersistedResourceRegistry {

        private static final String TOKEN_FIELD = "@token";

        private final Map<Integer, IJsonSerializable> byToken = new HashMap<>();
        private final Map<String, IJsonSerializable> byClass = new HashMap<>();
        private final AtomicInteger nextToken = new AtomicInteger();

        /** @return a stub of {@code clazz} that serializes to a token this registry resolves back to it. */
        <T extends IJsonSerializable> T serializable(Class<T> clazz) {
            T instance = mock(clazz);
            int token = nextToken.incrementAndGet();
            byToken.put(token, instance);
            ObjectNode node = MAPPER.createObjectNode();
            node.put(TOKEN_FIELD, token);
            try {
                when(instance.toJson(any())).thenReturn(node);
            } catch (HyracksDataException e) {
                throw new AssertionError(e);
            }
            return instance;
        }

        @Override
        public ObjectNode getClassIdentifier(Class<? extends IJsonSerializable> clazz, long version) {
            ObjectNode node = MAPPER.createObjectNode();
            node.put(TYPE_FIELD_ID, clazz.getSimpleName());
            node.put(VERSION_FIELD_ID, version);
            node.put(CLASS_FIELD_ID, clazz.getName());
            return node;
        }

        @Override
        public IJsonSerializable deserialize(JsonNode json) throws HyracksDataException {
            if (json.has(TOKEN_FIELD)) {
                return byToken.get(json.get(TOKEN_FIELD).asInt());
            }
            String className = json.get(CLASS_FIELD_ID).asText();
            IJsonSerializable known = byClass.get(className);
            if (known != null) {
                return known;
            }
            throw HyracksDataException.create(new IllegalStateException("unregistered class " + className));
        }

        @Override
        public IJsonSerializable deserializeOrDefault(JsonNode json, Class<? extends IJsonSerializable> clazz)
                throws HyracksDataException {
            return json == null ? null : deserialize(json);
        }

        FakeRegistry() {
            byClass.put(NoOpIOOperationCallbackFactory.class.getName(), NoOpIOOperationCallbackFactory.INSTANCE);
            byClass.put(NoOpPageWriteCallbackFactory.class.getName(), NoOpPageWriteCallbackFactory.INSTANCE);
            byClass.put(AppendOnlyLinkedMetadataPageManagerFactory.class.getName(),
                    AppendOnlyLinkedMetadataPageManagerFactory.INSTANCE);
            byClass.put(TestDoubleArrayVectorAccessor.Factory.class.getName(),
                    TestDoubleArrayVectorAccessor.Factory.INSTANCE);
            byClass.put(TestVTreeDistanceFunctionFactory.class.getName(), TestVTreeDistanceFunctionFactory.INSTANCE);
        }
    }
}
