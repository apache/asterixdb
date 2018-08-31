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

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.ILSMComponentIdGeneratorFactory;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGenerator;
import org.apache.hyracks.storage.common.IResource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * This factory implementation is used by AsterixDB layer so that indexes of a dataset (/partition)
 * use the same Id generator. This guarantees their memory components would receive the same Id upon
 * activation.
 */
public class DatasetLSMComponentIdGeneratorFactory implements ILSMComponentIdGeneratorFactory {
    private static final long serialVersionUID = 1L;

    private final int datasetId;

    public DatasetLSMComponentIdGeneratorFactory(int datasetId) {
        this.datasetId = datasetId;
    }

    @Override
    public ILSMComponentIdGenerator getComponentIdGenerator(INCServiceContext serviceCtx, IResource resource)
            throws HyracksDataException {
        IDatasetLifecycleManager dslcManager =
                ((INcApplicationContext) serviceCtx.getApplicationContext()).getDatasetLifecycleManager();
        int partition = StoragePathUtil.getPartitionNumFromRelativePath(resource.getPath());
        return dslcManager.getComponentIdGenerator(datasetId, partition, resource.getPath());
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.put("datasetId", datasetId);
        return json;
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        return new DatasetLSMComponentIdGeneratorFactory(json.get("datasetId").asInt());
    }
}
