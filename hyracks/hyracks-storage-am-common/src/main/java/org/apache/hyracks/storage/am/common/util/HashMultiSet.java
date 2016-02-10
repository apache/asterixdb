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

package org.apache.hyracks.storage.am.common.util;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Quick and dirty implementation of a HashMultiSet backed by a HashMap.
 * It only implements a minimal subset of the collection interface to make our tests work.
 */
public class HashMultiSet<E> extends AbstractCollection<E> {

    private final Map<E, List<E>> map = new HashMap<E, List<E>>();
    private int size = 0;

    @Override
    public boolean add(E e) {
        List<E> list = map.get(e);
        if (list == null) {
            list = new ArrayList<E>();
            map.put(e, list);
        }
        list.add(e);
        size++;
        return true;
    }

    @Override
    public boolean contains(Object o) {
        return map.containsKey(o);
    }

    @Override
    public boolean remove(Object o) {
        List<E> list = map.get(o);
        if (list == null) {
            return false;
        }
        list.remove(list.size() - 1);
        if (list.isEmpty()) {
            map.remove(o);
        }
        size--;
        return true;
    }

    @Override
    public Iterator<E> iterator() {
        return new HashMultiSetIterator();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void clear() {
        map.clear();
        size = 0;
    }

    private class HashMultiSetIterator implements Iterator<E> {

        private Iterator<Map.Entry<E, List<E>>> mapIter;
        private Iterator<E> listIter;

        public HashMultiSetIterator() {
            mapIter = map.entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
            if (mapIter.hasNext() || (listIter != null && listIter.hasNext())) {
                return true;
            }
            return false;
        }

        @Override
        public E next() {
            if (listIter == null || (listIter != null && !listIter.hasNext())) {
                Map.Entry<E, List<E>> entry = mapIter.next();
                listIter = entry.getValue().iterator();
                return listIter.next();
            }
            return listIter.next();
        }

        @Override
        public void remove() {
            throw new IllegalStateException("Not implemented");
        }
    }
}
