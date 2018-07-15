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

package org.apache.asterix.common.ioopcallbacks;

import org.apache.asterix.common.api.IDatasetInfoProvider;
import org.apache.asterix.common.api.ILSMComponentIdGeneratorFactory;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGenerator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.common.IResource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class LSMIndexIOOperationCallbackFactory implements ILSMIOOperationCallbackFactory {

    private static final long serialVersionUID = 1L;

    protected final ILSMComponentIdGeneratorFactory idGeneratorFactory;

    protected final IDatasetInfoProvider datasetInfoProvider;

    protected transient INCServiceContext ncCtx;

    protected transient IResource resource;

    public LSMIndexIOOperationCallbackFactory(ILSMComponentIdGeneratorFactory idGeneratorFactory,
            IDatasetInfoProvider datasetInfoProvider) {
        this.idGeneratorFactory = idGeneratorFactory;
        this.datasetInfoProvider = datasetInfoProvider;
    }

    @Override
    public void initialize(INCServiceContext ncCtx, IResource resource) {
        this.ncCtx = ncCtx;
        this.resource = resource;
    }

    protected ILSMComponentIdGenerator getComponentIdGenerator() throws HyracksDataException {
        return idGeneratorFactory.getComponentIdGenerator(ncCtx, resource);
    }

    protected IIndexCheckpointManagerProvider getIndexCheckpointManagerProvider() {
        return ((INcApplicationContext) ncCtx.getApplicationContext()).getIndexCheckpointManagerProvider();
    }

    @Override
    public ILSMIOOperationCallback createIoOpCallback(ILSMIndex index) throws HyracksDataException {
        return new LSMIOOperationCallback(datasetInfoProvider.getDatasetInfo(ncCtx), index,
                getComponentIdGenerator().getId(), getIndexCheckpointManagerProvider());
    }

    @Override
    public int getCurrentMemoryComponentIndex() throws HyracksDataException {
        return idGeneratorFactory.getComponentIdGenerator(ncCtx, resource).getCurrentComponentIndex();
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.set("idGeneratorFactory", idGeneratorFactory.toJson(registry));
        json.set("datasetInfoProvider", datasetInfoProvider.toJson(registry));
        return json;
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        final ILSMComponentIdGeneratorFactory idGeneratorFactory =
                (ILSMComponentIdGeneratorFactory) registry.deserialize(json.get("idGeneratorFactory"));
        final IDatasetInfoProvider datasetInfoProvider =
                (IDatasetInfoProvider) registry.deserialize(json.get("datasetInfoProvider"));
        return new LSMIndexIOOperationCallbackFactory(idGeneratorFactory, datasetInfoProvider);
    }
}
