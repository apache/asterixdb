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
package org.apache.asterix.common.dataflow;

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IResource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A local resource with a dataset id and an assigned partition
 */
public class DatasetLocalResource implements IResource {

    private static final long serialVersionUID = 1L;
    /**
     * The dataset id
     */
    private final int datasetId;
    /**
     * The resource partition
     */
    private final int partition;
    private final IResource resource;

    public DatasetLocalResource(int datasetId, int partition, IResource resource) {
        this.datasetId = datasetId;
        this.partition = partition;
        this.resource = resource;
    }

    public int getPartition() {
        return partition;
    }

    public int getDatasetId() {
        return datasetId;
    }

    @Override
    public String getPath() {
        return resource.getPath();
    }

    @Override
    public void setPath(String path) {
        resource.setPath(path);
    }

    @Override
    public IIndex createInstance(INCServiceContext ncServiceCtx) throws HyracksDataException {
        return resource.createInstance(ncServiceCtx);
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.put("datasetId", datasetId);
        json.put("partition", partition);
        json.set("resource", resource.toJson(registry));
        return json;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        final int datasetId = json.get("datasetId").asInt();
        final int partition = json.get("partition").asInt();
        final IResource resource = (IResource) registry.deserialize(json.get("resource"));
        return new DatasetLocalResource(datasetId, partition, resource);
    }
}
