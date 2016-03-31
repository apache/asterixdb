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
package org.apache.hyracks.storage.common.file;

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public class TransientLocalResourceRepository implements ILocalResourceRepository {

    private Map<String, LocalResource> path2ResourceMap = new HashMap<String, LocalResource>();
    private Map<Long, LocalResource> id2ResourceMap = new HashMap<Long, LocalResource>();

    @Override
    public LocalResource getResourceByPath(String path) throws HyracksDataException {
        return path2ResourceMap.get(path);
    }

    @Override
    public synchronized void insert(LocalResource resource) throws HyracksDataException {
        long id = resource.getResourceId();

        if (id2ResourceMap.containsKey(id)) {
            throw new HyracksDataException("Duplicate resource");
        }
        id2ResourceMap.put(id, resource);
        path2ResourceMap.put(resource.getResourcePath(), resource);
    }

    @Override
    public synchronized void deleteResourceByPath(String path) throws HyracksDataException {
        LocalResource resource = path2ResourceMap.get(path);
        if (resource == null) {
            throw new HyracksDataException("Resource doesn't exist");
        }
        id2ResourceMap.remove(resource.getResourceId());
        path2ResourceMap.remove(path);
    }

    @Override
    public long getMaxResourceID() throws HyracksDataException {
        long maxResourceId = 0;

        for (Long resourceId : id2ResourceMap.keySet()) {
            maxResourceId = Math.max(maxResourceId, resourceId);
        }
        return maxResourceId;
    }
}
