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
package org.apache.asterix.metadata.lock;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.ExternalDatasetAccessManager;

/**
 * This is a singelton class used to maintain the version of each external dataset with indexes
 * It should be consolidated once a better global dataset lock management is introduced.
 *
 * @author alamouda
 */
public class ExternalDatasetsRegistry {
    public static final ExternalDatasetsRegistry INSTANCE = new ExternalDatasetsRegistry();
    private final ConcurrentHashMap<String, ExternalDatasetAccessManager> globalRegister;

    private ExternalDatasetsRegistry() {
        globalRegister = new ConcurrentHashMap<>();
    }

    /**
     * Get the current version of the dataset
     *
     * @param dataset
     * @return
     */
    public int getDatasetVersion(Dataset dataset) {
        String key = dataset.getDataverseName() + "." + dataset.getDatasetName();
        ExternalDatasetAccessManager datasetAccessMgr = globalRegister.get(key);
        if (datasetAccessMgr == null) {
            globalRegister.putIfAbsent(key, new ExternalDatasetAccessManager());
            datasetAccessMgr = globalRegister.get(key);
        }
        return datasetAccessMgr.getVersion();
    }

    public int getAndLockDatasetVersion(Dataset dataset, MetadataProvider metadataProvider) {

        Map<String, Integer> locks;
        String lockKey = dataset.getDataverseName() + "." + dataset.getDatasetName();
        // check first if the lock was aquired already
        locks = metadataProvider.getExternalDataLocks();
        if (locks == null) {
            locks = new HashMap<>();
            metadataProvider.setExternalDataLocks(locks);
        } else {
            // if dataset was accessed already by this job, return the registered version
            Integer version = locks.get(lockKey);
            if (version != null) {
                return version;
            }
        }

        ExternalDatasetAccessManager datasetAccessMgr = globalRegister.get(lockKey);
        if (datasetAccessMgr == null) {
            globalRegister.putIfAbsent(lockKey, new ExternalDatasetAccessManager());
            datasetAccessMgr = globalRegister.get(lockKey);
        }

        // aquire the correct lock
        int version = datasetAccessMgr.queryBegin();
        locks.put(lockKey, version);
        return version;
    }

    public void refreshBegin(Dataset dataset) {
        String key = dataset.getDataverseName() + "." + dataset.getDatasetName();
        ExternalDatasetAccessManager datasetAccessMgr = globalRegister.get(key);
        if (datasetAccessMgr == null) {
            datasetAccessMgr = globalRegister.put(key, new ExternalDatasetAccessManager());
        }
        // aquire the correct lock
        datasetAccessMgr.refreshBegin();
    }

    public void removeDatasetInfo(Dataset dataset) {
        String key = dataset.getDataverseName() + "." + dataset.getDatasetName();
        globalRegister.remove(key);
    }

    public void refreshEnd(Dataset dataset, boolean success) {
        String key = dataset.getDataverseName() + "." + dataset.getDatasetName();
        globalRegister.get(key).refreshEnd(success);
    }

    public void buildIndexBegin(Dataset dataset, boolean firstIndex) {
        String key = dataset.getDataverseName() + "." + dataset.getDatasetName();
        ExternalDatasetAccessManager datasetAccessMgr = globalRegister.get(key);
        if (datasetAccessMgr == null) {
            globalRegister.putIfAbsent(key, new ExternalDatasetAccessManager());
            datasetAccessMgr = globalRegister.get(key);
        }
        // aquire the correct lock
        datasetAccessMgr.buildIndexBegin(firstIndex);
    }

    public void buildIndexEnd(Dataset dataset, boolean firstIndex) {
        String key = dataset.getDataverseName() + "." + dataset.getDatasetName();
        globalRegister.get(key).buildIndexEnd(firstIndex);
    }

    public void releaseAcquiredLocks(MetadataProvider metadataProvider) {
        Map<String, Integer> locks = metadataProvider.getExternalDataLocks();
        if (locks == null) {
            return;
        } else {
            // if dataset was accessed already by this job, return the registered version
            Set<Entry<String, Integer>> aquiredLocks = locks.entrySet();
            for (Entry<String, Integer> entry : aquiredLocks) {
                ExternalDatasetAccessManager accessManager = globalRegister.get(entry.getKey());
                if (accessManager != null) {
                    accessManager.queryEnd(entry.getValue());
                }
            }
            locks.clear();
        }
    }
}
