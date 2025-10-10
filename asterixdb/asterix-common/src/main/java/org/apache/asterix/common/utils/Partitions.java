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

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

/**
 * A specialized bit set for storing partition IDs.
 * Partition IDs are in the range [-1, 32766] (inclusive).
 * The value -1 is used to represent the "unpartitioned" partition.
 * The value 32767 is reserved for the internal shift and cannot be used as a partition ID.
 * This class internally shifts the partition IDs by +1 to fit into an unsigned short range [0, 32767].
 */
public class Partitions implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private static final int MAX_PARTITION_ID = Integer.MAX_VALUE - 1;
    private static final int MIN_PARTITION_ID = -1;
    private static final short MINUS_ONE = (short) -1;
    private final IntSortedSet delegate;

    public Partitions() {
        delegate = new IntSortedBitSet();
    }

    public Partitions(int initialMaxValue) {
        delegate = new IntSortedBitSet(initialMaxValue + 1);
    }

    public Partitions(IntSortedSet delegate) {
        this.delegate = delegate;
    }

    /**
     * Adds a partition ID to the set. Partition ID must be in the range [-1, 32766].
     * @param k the partition ID to add
     * @return true if the partition ID was added, false if it was already present
     * @throws IllegalArgumentException if the partition ID is out of range
     */
    public boolean add(int k) {
        if (k > MAX_PARTITION_ID || k < MIN_PARTITION_ID) {
            throw new IllegalArgumentException("Partition number " + k + " out of range");
        }
        return delegate.add((short) (k + 1));
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
        if (!(o instanceof Partitions other)) {
            return false;
        }
        return delegate.equals(other.delegate);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public String toString() {
        if (isEmpty()) {
            return "[]";
        }
        IntIterator iter = delegate.iterator();
        StringBuilder builder = new StringBuilder();
        builder.append('[');
        if (delegate.firstInt() == 0) {
            builder.append(MINUS_ONE);
            iter.nextInt(); // this is the zero we just printed
            if (iter.hasNext()) {
                builder.append(',');
            }
        }
        IntUtil.appendCompact(iter, builder, MINUS_ONE);
        builder.append(']');
        return builder.toString();
    }
}
