/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.storage.common.file;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class TransientLocalResourceRepository implements ILocalResourceRepository {

    private Map<String, LocalResource> name2ResourceMap = new HashMap<String, LocalResource>();
    private Map<Long, LocalResource> id2ResourceMap = new HashMap<Long, LocalResource>();

    @Override
    public LocalResource getResourceById(long id) throws HyracksDataException {
        return id2ResourceMap.get(id);
    }

    @Override
    public LocalResource getResourceByName(String name) throws HyracksDataException {
        return name2ResourceMap.get(name);
    }

    @Override
    public synchronized void insert(LocalResource resource) throws HyracksDataException {
        long id = resource.getResourceId();

        if (id2ResourceMap.containsKey(id)) {
            throw new HyracksDataException("Duplicate resource");
        }
        id2ResourceMap.put(id, resource);
        name2ResourceMap.put(resource.getResourceName(), resource);
    }

    @Override
    public synchronized void deleteResourceById(long id) throws HyracksDataException {
        LocalResource resource = id2ResourceMap.get(id);
        if (resource == null) {
            throw new HyracksDataException("Resource doesn't exist");
        }
        id2ResourceMap.remove(id);
        name2ResourceMap.remove(resource.getResourceName());
    }

    @Override
    public synchronized void deleteResourceByName(String name) throws HyracksDataException {
        LocalResource resource = name2ResourceMap.get(name);
        if (resource == null) {
            throw new HyracksDataException("Resource doesn't exist");
        }
        id2ResourceMap.remove(resource.getResourceId());
        name2ResourceMap.remove(name);
    }

    @Override
    public List<LocalResource> getAllResources() throws HyracksDataException {
        List<LocalResource> resources = new ArrayList<LocalResource>();
        for (LocalResource resource : id2ResourceMap.values()) {
            resources.add(resource);
        }
        return resources;
    }
}
