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
package org.apache.asterix.common.transactions;

import java.io.Serializable;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.common.file.LocalResource;

/**
 * The base resource that will be written to disk. it will go in the serializable resource
 * member in {@link LocalResource}
 */
public abstract class Resource implements Serializable {

    private static final long serialVersionUID = 1L;
    private final int datasetId;
    private final int partition;
    protected final ITypeTraits[] filterTypeTraits;
    protected final IBinaryComparatorFactory[] filterCmpFactories;
    protected final int[] filterFields;

    public Resource(int datasetId, int partition, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields) {
        this.datasetId = datasetId;
        this.partition = partition;
        this.filterTypeTraits = filterTypeTraits;
        this.filterCmpFactories = filterCmpFactories;
        this.filterFields = filterFields;
    }

    public int partition() {
        return partition;
    }

    public int datasetId() {
        return datasetId;
    }

    public abstract ILSMIndex createIndexInstance(IAsterixAppRuntimeContextProvider runtimeContextProvider,
            LocalResource resource) throws HyracksDataException;
}
