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

    private static Map<ByteBuffer, Object> resourceRepository = new HashMap<ByteBuffer, Object>(); // repository
    // containing
    // resources
    // that
    // participate
    // in
    // transactions

    private static Map<Byte, IResourceManager> resourceMgrRepository = new HashMap<Byte, IResourceManager>(); // repository

    // containing
    // resource
    // managers

    public static void registerTransactionalResource(byte[] resourceBytes, Object resource) {
        ByteBuffer resourceId = ByteBuffer.wrap(resourceBytes); // need to
        // convert to
        // ByteBuffer so
        // that a byte[]
        // can be used
        // as a key in a
        // hash map.
        synchronized (resourceRepository) {
            if (resourceRepository.get(resourceId) == null) {
                resourceRepository.put(resourceId, resource);
                resourceRepository.notifyAll(); // notify all reader threads
                // that are waiting to retrieve
                // a resource from the
                // repository

            }
        }
    }

    public static void registerTransactionalResourceManager(byte id, IResourceManager resourceMgr) {
        synchronized (resourceMgrRepository) {
            if (resourceMgrRepository.get(id) == null) {
                resourceMgrRepository.put(id, resourceMgr);
                resourceMgrRepository.notifyAll(); // notify all reader threads
                // that are waiting to
                // retrieve a resource
                // manager from the
                // repository
            }
        }
    }

    public static Object getTransactionalResource(byte[] resourceIdBytes) {
        ByteBuffer buffer = ByteBuffer.wrap(resourceIdBytes);
        synchronized (resourceRepository) {
            while (resourceRepository.get(buffer) == null) {
                try {
                    resourceRepository.wait();
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                    break; // the thread might be interrupted due to other
                    // failures occurring elsewhere, break from the loop
                }
            }
            return resourceRepository.get(buffer);
        }
    }

    public static IResourceManager getTransactionalResourceMgr(byte id) {
        synchronized (resourceMgrRepository) {
            while (resourceMgrRepository.get(id) == null) {
                try {
                    resourceMgrRepository.wait();
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                    break; // the thread might be interrupted due to other
                    // failures occurring elsewhere, break from the loop
                }
            }
            return resourceMgrRepository.get(id);
        }

    }

}
