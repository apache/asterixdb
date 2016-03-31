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
package org.apache.hyracks.algebricks.common.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ListSet<E> implements Set<E> {
    private List<E> list = new ArrayList<E>();

    public ListSet() {
    }

    public ListSet(Collection<? extends E> arg) {
        this.addAll(arg);
    }

    @Override
    public boolean add(E arg0) {
        if (list.contains(arg0))
            return false;
        return list.add(arg0);
    }

    @Override
    public boolean addAll(Collection<? extends E> arg0) {
        for (E item : arg0)
            if (list.contains(item))
                return false;
        return list.addAll(arg0);
    }

    @Override
    public void clear() {
        list.clear();
    }

    @Override
    public boolean contains(Object arg0) {
        return list.contains(arg0);
    }

    @Override
    public boolean containsAll(Collection<?> arg0) {
        return list.containsAll(arg0);
    }

    @Override
    public boolean isEmpty() {
        return list.isEmpty();
    }

    @Override
    public Iterator<E> iterator() {
        return list.iterator();
    }

    @Override
    public boolean remove(Object arg0) {
        return list.remove(arg0);
    }

    @Override
    public boolean removeAll(Collection<?> arg0) {
        return list.removeAll(arg0);
    }

    @Override
    public boolean retainAll(Collection<?> arg0) {
        return list.retainAll(arg0);
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public Object[] toArray() {
        return list.toArray();
    }

    @Override
    public <T> T[] toArray(T[] arg0) {
        return list.toArray(arg0);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object arg) {
        if (!(arg instanceof Set))
            return false;
        Set<E> set = (Set<E>) arg;
        for (E item : set) {
            if (!this.contains(item))
                return false;
        }
        for (E item : this) {
            if (!set.contains(item))
                return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return list.toString();
    }

}
