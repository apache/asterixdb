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
package org.apache.asterix.app.nc;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.storage.IIndexCheckpointManager;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.storage.ResourceReference;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;

public class IndexCheckpointManagerProvider implements IIndexCheckpointManagerProvider {

    private final Map<ResourceReference, IndexCheckpointManager> indexCheckpointManagerMap = new HashMap<>();
    private final IIOManager ioManager;

    public IndexCheckpointManagerProvider(IIOManager ioManager) {
        this.ioManager = ioManager;
    }

    @Override
    public IIndexCheckpointManager get(ResourceReference ref) throws HyracksDataException {
        synchronized (indexCheckpointManagerMap) {
            return indexCheckpointManagerMap.computeIfAbsent(ref, this::create);
        }
    }

    @Override
    public void close(ResourceReference ref) {
        synchronized (indexCheckpointManagerMap) {
            indexCheckpointManagerMap.remove(ref);
        }
    }

    private IndexCheckpointManager create(ResourceReference ref) {
        try {
            final Path indexPath = StoragePathUtil.getIndexPath(ioManager, ref);
            return new IndexCheckpointManager(indexPath);
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }
}
