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
package org.apache.asterix.common.utils;

import java.io.Serial;
import java.io.Serializable;
import java.util.function.IntPredicate;

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

/**
 * A specialized set for storing dataset IDs.
 */
public class Datasets implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private final IntSortedSet delegate;

    public Datasets() {
        this.delegate = new IntAVLTreeSet();
    }

    public boolean add(int k) {
        return delegate.add(k);
    }

    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    public void clear() {
        delegate.clear();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Datasets other)) {
            return false;
        }
        return delegate.equals(other.delegate);
    }

    public boolean contains(int i) {
        return delegate.contains(i);
    }

    public boolean addAll(IntCollection intCollection) {
        return delegate.addAll(intCollection);
    }

    public boolean containsAll(IntCollection intCollection) {
        return delegate.containsAll(intCollection);
    }

    public boolean removeIf(IntPredicate filter) {
        return delegate.removeIf(filter);
    }

    public boolean removeAll(IntCollection intCollection) {
        return delegate.removeAll(intCollection);
    }

    public boolean retainAll(IntCollection intCollection) {
        return delegate.retainAll(intCollection);
    }

    public int[] toArray() {
        return delegate.toIntArray();
    }

    public int size() {
        return delegate.size();
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public String toString() {
        return IntUtil.toCompactString(delegate.iterator());
    }
}
