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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.transaction.management.service.transaction.IResourceManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.MutableResourceId;

/**
 * Represents a repository containing Resource Managers and Resources in the
 * transaction eco-system. Operations on a resource require acquiring
 * appropriate locks (for isolation) and writing logs (durability). Every
 * resource is managed by an associated resource manager that contains the logic
 * to interpret the logs and take necessary action(s) during roll back or
 * recovery. An example of resource is a @see ITreeIndex that is managed by a
 * resource manager @see TreeResourceManager
 */
public class TransactionalResourceRepository {

    private Map<MutableResourceId, Object> resourceRepository = new HashMap<MutableResourceId, Object>(); // repository

    private Map<Byte, IResourceManager> resourceMgrRepository = new HashMap<Byte, IResourceManager>(); // repository
    
    private MutableResourceId mutableResourceId = new MutableResourceId(0);

    public void registerTransactionalResource(long resourceId, Object resource) {
        synchronized (resourceRepository) {
            mutableResourceId.setId(resourceId);
            if (resourceRepository.get(resourceId) == null) {
                MutableResourceId newMutableResourceId = new MutableResourceId(resourceId);
                resourceRepository.put(newMutableResourceId, resource);
                
                // wake up threads waiting for the resource
                resourceRepository.notifyAll();
            }
        }
    }

    public void registerTransactionalResourceManager(byte id, IResourceManager resourceMgr) {
        synchronized (resourceMgrRepository) {
            if (resourceMgrRepository.get(id) == null) {
                resourceMgrRepository.put(id, resourceMgr);
                
                // wake up threads waiting for the resource manager
                resourceMgrRepository.notifyAll();
            }
        }
    }

    public Object getTransactionalResource(long resourceId) {
        synchronized (resourceRepository) {
            mutableResourceId.setId(resourceId);
            while (resourceRepository.get(mutableResourceId) == null) {
                try {
                    resourceRepository.wait();
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                    break; // the thread might be interrupted due to other
                    // failures occurring elsewhere, break from the loop
                }
            }
            return resourceRepository.get(mutableResourceId);
        }
    }

    public IResourceManager getTransactionalResourceMgr(byte id) {
        synchronized (resourceMgrRepository) {
            return resourceMgrRepository.get(id);
        }

    }

}
