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
package edu.uci.ics.hyracks.algebricks.runtime.context;

import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;

public class AsterixBTreeRegistry {

    private HashMap<Integer, BTree> map = new HashMap<Integer, BTree>();
    private Lock registryLock = new ReentrantLock();

    public BTree get(int fileId) {
        return map.get(fileId);
    }

    // TODO: not very high concurrency, but good enough for now
    public void lock() {
        registryLock.lock();
    }

    public void unlock() {
        registryLock.unlock();
    }

    public void register(int fileId, BTree btree) {
        map.put(fileId, btree);
    }

    public void unregister(int fileId) {
        map.remove(fileId);
    }
}
