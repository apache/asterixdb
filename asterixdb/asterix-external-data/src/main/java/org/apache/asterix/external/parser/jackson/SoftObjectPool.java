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

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;

import org.apache.asterix.om.util.container.IObjectFactory;

/**
 * Object pool for DFS traversal mode, which allows to recycle objects
 * as soon as it is not needed.
 */
public class SoftObjectPool<E, T> extends AbstractObjectPool<E, T, SoftObjectPool.SizedReference<E>> {
    // where the garbage collector leaves the references it clears; null when the pool is unmeasured
    private final ReferenceQueue<E> cleared;
    private final AtomicLong reclaimedBytes = new AtomicLong();

    public SoftObjectPool() {
        this(null, null);
    }

    public SoftObjectPool(IObjectFactory<E, T> objectFactory) {
        this(objectFactory, null);
    }

    public SoftObjectPool(IObjectFactory<E, T> objectFactory, T element) {
        this(objectFactory, element, null);
    }

    public SoftObjectPool(IObjectFactory<E, T> objectFactory, T element, ToLongFunction<E> sizer) {
        super(objectFactory, element, sizer);
        cleared = isMeasured() ? new ReferenceQueue<>() : null;
    }

    @Override
    protected E unwrap(SizedReference<E> wrapped) {
        if (wrapped == null) {
            return null;
        }
        E element = wrapped.get();
        if (element != null) {
            borrowed(wrapped.size);
        } else {
            // cleared entries are counted off as they leave the queue, never here, so that each is counted once
            expungeCleared();
        }
        return element;
    }

    @Override
    protected SizedReference<E> wrap(E element, int size) {
        return new SizedReference<>(element, size, cleared);
    }

    @Override
    public long getPooledBytes() {
        expungeCleared();
        // an entry recycled and reclaimed since the last publish is in reclaimedBytes only, hence the floor
        return Math.max(0, super.getPooledBytes() - reclaimedBytes.get());
    }

    /**
     * Counts off the entries the garbage collector has reclaimed. May be called from any thread.
     */
    @SuppressWarnings("unchecked")
    private void expungeCleared() {
        if (cleared == null) {
            return;
        }
        long reclaimed = 0;
        for (Reference<? extends E> ref = cleared.poll(); ref != null; ref = cleared.poll()) {
            reclaimed += ((SizedReference<E>) ref).size;
        }
        if (reclaimed != 0) {
            reclaimedBytes.addAndGet(reclaimed);
        }
    }

    static final class SizedReference<E> extends SoftReference<E> {
        // an int fits in the padding of a SoftReference, so that keeping the size does not enlarge the entry
        private final int size;

        SizedReference(E referent, int size, ReferenceQueue<E> queue) {
            super(referent, queue);
            this.size = size;
        }
    }
}
