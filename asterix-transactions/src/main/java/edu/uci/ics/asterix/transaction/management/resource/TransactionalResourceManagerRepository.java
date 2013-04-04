/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.asterix.transaction.management.resource;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.transaction.management.service.transaction.IResourceManager;

/**
 * Represents a repository containing Resource Managers and Resources in the
 * transaction eco-system. Operations on a resource require acquiring
 * appropriate locks (for isolation) and writing logs (durability). Every
 * resource is managed by an associated resource manager that contains the logic
 * to interpret the logs and take necessary action(s) during roll back or
 * recovery. An example of resource is a @see ITreeIndex that is managed by a
 * resource manager @see TreeResourceManager
 */
public class TransactionalResourceManagerRepository {

    private Map<Byte, IResourceManager> resourceMgrRepository = new HashMap<Byte, IResourceManager>(); // repository

    public void registerTransactionalResourceManager(byte id, IResourceManager resourceMgr) {
        synchronized (resourceMgrRepository) {
            if (resourceMgrRepository.get(id) == null) {
                resourceMgrRepository.put(id, resourceMgr);
            }
        }
    }

    public IResourceManager getTransactionalResourceMgr(byte id) {
        synchronized (resourceMgrRepository) {
            return resourceMgrRepository.get(id);
        }
    }
}
