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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util;

import java.util.ArrayList;

import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IObjectFactory;

public class ObjectCache<T> {
    protected final int expandSize;
    protected final IObjectFactory<T> objFactory;
    protected final ArrayList<T> cache;
    protected int lastReturned = 0;

    public ObjectCache(IObjectFactory<T> objFactory, int initialSize, int expandSize) {
        this.objFactory = objFactory;
        this.cache = new ArrayList<T>(initialSize);
        this.expandSize = expandSize;
        expand(initialSize);
    }

    private void expand(int expandSize) {
        for (int i = 0; i < expandSize; i++) {
            cache.add(objFactory.create());
        }
    }

    public void reset() {
        lastReturned = 0;
    }

    public T getNext() {
        if (lastReturned >= cache.size()) {
            expand(expandSize);
        }
        return cache.get(lastReturned++);
    }
}
