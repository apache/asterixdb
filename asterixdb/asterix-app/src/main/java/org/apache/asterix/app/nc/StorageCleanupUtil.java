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

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class StorageCleanupUtil {

    private static final Logger LOGGER = LogManager.getLogger();

    private StorageCleanupUtil() {
    }

    /**
     * Removes the index at {@code resourcePath}: unregisters it if open, deletes its resource, destroys it so its
     * component files are unmapped as well as removed. A directory with no resource behind it is deleted outright;
     * an index that cannot be instantiated loses its resource but keeps its files, as before.
     */
    public static void deleteIndex(INcApplicationContext appCtx, String resourcePath) throws HyracksDataException {
        IDatasetLifecycleManager lcManager = appCtx.getDatasetLifecycleManager();
        ILocalResourceRepository repository = appCtx.getLocalResourceRepository();
        IIOManager ioManager = appCtx.getServiceContext().getIoManager();
        FileReference indexDir = ioManager.resolve(resourcePath);
        synchronized (lcManager) {
            IIndex index = lcManager.get(resourcePath);
            if (index != null) {
                LOGGER.warn("unregistering index {}", resourcePath);
                lcManager.unregister(resourcePath);
            } else {
                LocalResource lr = repository.get(resourcePath);
                if (lr == null) {
                    LOGGER.warn("no resource for index {}; deleting its directory", resourcePath);
                    ioManager.delete(indexDir);
                    return;
                }
                try {
                    index = lr.getResource().createInstance(appCtx.getServiceContext());
                } catch (Exception e) {
                    LOGGER.warn("failed to initialize index {}", resourcePath, e);
                }
            }
            repository.delete(resourcePath);
            if (index != null) {
                index.destroy();
            }
        }
    }
}
