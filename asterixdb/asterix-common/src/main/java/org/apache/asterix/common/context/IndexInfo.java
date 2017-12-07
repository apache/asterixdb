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

import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.common.LocalResource;

public class IndexInfo extends Info {
    private final ILSMIndex index;
    private final int datasetId;
    private final long resourceId;
    private final int partition;
    private final LocalResource localResource;

    public IndexInfo(ILSMIndex index, int datasetId, LocalResource localResource, int partition) {
        this.index = index;
        this.datasetId = datasetId;
        this.localResource = localResource;
        this.resourceId = localResource.getId();
        this.partition = partition;
    }

    public ILSMIndex getIndex() {
        return index;
    }

    public long getResourceId() {
        return resourceId;
    }

    public int getPartition() {
        return partition;
    }

    public int getDatasetId() {
        return datasetId;
    }

    public LocalResource getLocalResource() {
        return localResource;
    }
}
