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
package org.apache.asterix.test.dataflow;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.context.DatasetLifecycleManager;
import org.apache.asterix.common.context.DatasetResource;
import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexOperationTrackerFactory;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.common.IResource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TestPrimaryIndexOperationTrackerFactory extends PrimaryIndexOperationTrackerFactory {

    private static final long serialVersionUID = 1L;
    private final int datasetId;

    public TestPrimaryIndexOperationTrackerFactory(int datasetId) {
        super(datasetId);
        this.datasetId = datasetId;
    }

    @Override
    public ILSMOperationTracker getOperationTracker(INCServiceContext ctx, IResource resource) {
        try {
            INcApplicationContext appCtx = (INcApplicationContext) ctx.getApplicationContext();
            DatasetLifecycleManager dslcManager = (DatasetLifecycleManager) appCtx.getDatasetLifecycleManager();
            DatasetResource dsr = dslcManager.getDatasetLifecycle(datasetId);
            int partition = StoragePathUtil.getPartitionNumFromRelativePath(resource.getPath());
            PrimaryIndexOperationTracker opTracker =
                    dslcManager.getOperationTracker(datasetId, partition, resource.getPath());
            if (!(opTracker instanceof TestPrimaryIndexOperationTracker)) {
                Field opTrackersField = DatasetResource.class.getDeclaredField("datasetPrimaryOpTrackers");
                opTracker = new TestPrimaryIndexOperationTracker(datasetId, partition,
                        appCtx.getTransactionSubsystem().getLogManager(), dsr.getDatasetInfo(),
                        dslcManager.getComponentIdGenerator(datasetId, partition, resource.getPath()));
                replaceMapEntry(opTrackersField, dsr, partition, opTracker);
            }
            return opTracker;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void setFinal(Field field, Object obj, Object newValue) throws Exception {
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(obj, newValue);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    static void replaceMapEntry(Field field, Object obj, Object key, Object value)
            throws Exception, IllegalAccessException {
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        Map map = (Map) field.get(obj);
        map.put(key, value);
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.put("datasetId", datasetId);
        return json;
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        return new TestPrimaryIndexOperationTrackerFactory(json.get("datasetId").asInt());
    }
}
