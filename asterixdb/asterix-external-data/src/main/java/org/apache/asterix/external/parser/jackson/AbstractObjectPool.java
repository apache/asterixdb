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
package org.apache.asterix.external.parser.jackson;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.ToLongFunction;

import org.apache.asterix.om.util.container.IObjectFactory;

/**
 * Object pool for DFS traversal mode, which allows to recycle objects
 * as soon as it is not needed.
 */
public abstract class AbstractObjectPool<E, T, Q> implements IObjectPool<E> {
    private final IObjectFactory<E, T> objectFactory;
    private final Deque<Q> recycledObjects;
    private final T param;
    private final ToLongFunction<E> sizer;
    // kept by the owning thread alone, so that borrowing and recycling cost no synchronization
    private long pooledBytes;
    private volatile long publishedBytes;

    /**
     * @param sizer
     *            measures a pooled object in bytes, or {@code null} to leave the pool unmeasured
     */
    protected AbstractObjectPool(IObjectFactory<E, T> objectFactory, T param, ToLongFunction<E> sizer) {
        this.objectFactory = objectFactory;
        recycledObjects = new ArrayDeque<>();
        this.param = param;
        this.sizer = sizer;
    }

    public E getInstance() {
        E instance = unwrap(recycledObjects.poll());
        if (objectFactory != null && instance == null) {
            instance = objectFactory.create(param);
        }
        return instance;
    }

    public void recycle(E object) {
        if (object != null) {
            int size = sizeOf(object);
            pooledBytes += size;
            // LIFO so each nesting level reuses its own instance; FIFO grows every instance to the outermost value
            recycledObjects.push(wrap(object, size));
        }
    }

    @Override
    public void publishPooledBytes() {
        publishedBytes = pooledBytes;
    }

    @Override
    public long getPooledBytes() {
        return publishedBytes;
    }

    protected final boolean isMeasured() {
        return sizer != null;
    }

    /**
     * @return the size of {@code element} in bytes, capped at {@link Integer#MAX_VALUE}
     */
    protected final int sizeOf(E element) {
        return sizer == null ? 0 : (int) Math.min(sizer.applyAsLong(element), Integer.MAX_VALUE);
    }

    /**
     * Removes {@code size} bytes from the pool's total, for an object that was borrowed.
     */
    protected final void borrowed(int size) {
        pooledBytes -= size;
    }

    Iterable<Q> pooledEntries() {
        return recycledObjects;
    }

    /**
     * @return the object held by {@code element}, or {@code null}; the object has left the pool and must be reported
     *         as {@link #borrowed(int) borrowed}
     */
    protected abstract E unwrap(Q element);

    protected abstract Q wrap(E element, int size);

    @Override
    public String toString() {
        return recycledObjects.toString();
    }
}
