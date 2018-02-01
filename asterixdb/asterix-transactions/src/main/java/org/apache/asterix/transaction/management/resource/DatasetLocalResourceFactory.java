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
package org.apache.asterix.transaction.management.resource;

import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.common.IResource;
import org.apache.hyracks.storage.common.IResourceFactory;

public class DatasetLocalResourceFactory implements IResourceFactory {

    private static final long serialVersionUID = 1L;
    private final int datasetId;
    private final IResourceFactory resourceFactory;

    public DatasetLocalResourceFactory(int datasetId, IResourceFactory resourceFactory) {
        this.datasetId = datasetId;
        this.resourceFactory = resourceFactory;
    }

    @Override
    public IResource createResource(FileReference fileRef) {
        int partition = StoragePathUtil.getPartitionNumFromRelativePath(fileRef.getRelativePath());
        IResource resource = resourceFactory.createResource(fileRef);
        return new DatasetLocalResource(datasetId, partition, resource);
    }
}
