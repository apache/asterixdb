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

package org.apache.hyracks.storage.am.lsm.invertedindex.util;

import java.util.ArrayList;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IObjectFactory;

public class ObjectCache<T> {
    protected final int expandSize;
    protected final IObjectFactory<T> objFactory;
    protected final ArrayList<T> cache;
    protected int lastReturned = 0;

    public ObjectCache(IObjectFactory<T> objFactory, int initialSize, int expandSize) throws HyracksDataException {
        this.objFactory = objFactory;
        this.cache = new ArrayList<T>(initialSize);
        this.expandSize = expandSize;
        expand(initialSize);
    }

    private void expand(int expandSize) throws HyracksDataException {
        for (int i = 0; i < expandSize; i++) {
            cache.add(objFactory.create());
        }
    }

    public void reset() {
        lastReturned = 0;
    }

    public T getNext() throws HyracksDataException {
        if (lastReturned >= cache.size()) {
            expand(expandSize);
        }
        return cache.get(lastReturned++);
    }
}
