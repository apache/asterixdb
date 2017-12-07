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

import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.ILocalResourceRepositoryFactory;

public class PersistentLocalResourceRepositoryFactory implements ILocalResourceRepositoryFactory {
    private final IIOManager ioManager;
    private final String nodeId;
    private final MetadataProperties metadataProperties;
    private final IIndexCheckpointManagerProvider indexCheckpointManagerProvider;

    public PersistentLocalResourceRepositoryFactory(IIOManager ioManager, String nodeId,
            MetadataProperties metadataProperties, IIndexCheckpointManagerProvider indexCheckpointManagerProvider) {
        this.ioManager = ioManager;
        this.nodeId = nodeId;
        this.metadataProperties = metadataProperties;
        this.indexCheckpointManagerProvider = indexCheckpointManagerProvider;
    }

    @Override
    public ILocalResourceRepository createRepository() throws HyracksDataException {
        return new PersistentLocalResourceRepository(ioManager, nodeId, metadataProperties,
                indexCheckpointManagerProvider);
    }
}
