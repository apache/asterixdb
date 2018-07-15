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
package org.apache.asterix.common.context;

import java.util.List;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCacheProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AsterixVirtualBufferCacheProvider implements IVirtualBufferCacheProvider {

    private static final long serialVersionUID = 1L;
    private final int datasetId;

    public AsterixVirtualBufferCacheProvider(int datasetId) {
        this.datasetId = datasetId;
    }

    @Override
    public List<IVirtualBufferCache> getVirtualBufferCaches(INCServiceContext ctx, FileReference fileRef)
            throws HyracksDataException {
        IIOManager ioManager = ctx.getIoManager();
        int deviceId = getDeviceId(ioManager, fileRef);
        return ((INcApplicationContext) ctx.getApplicationContext()).getDatasetLifecycleManager()
                .getVirtualBufferCaches(datasetId, deviceId);
    }

    public static int getDeviceId(IIOManager ioManager, FileReference fileRef) {
        IODeviceHandle device = fileRef.getDeviceHandle();
        List<IODeviceHandle> devices = ioManager.getIODevices();
        int deviceId = 0;
        for (int i = 0; i < devices.size(); i++) {
            IODeviceHandle next = devices.get(i);
            if (next == device) {
                deviceId = i;
            }
        }
        return deviceId;
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.put("datasetId", datasetId);
        return json;
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        return new AsterixVirtualBufferCacheProvider(json.get("datasetId").asInt());
    }

}
