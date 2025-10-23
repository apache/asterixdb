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
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableBoolean;

import it.unimi.dsi.fastutil.ints.IntIterable;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import it.unimi.dsi.fastutil.ints.IntSortedSets;

/**
 * A specialized data structure for efficiently storing partitions.
 * Partitions are in the range [-1, 2_147_483_646] (inclusive).
 * The value -1 is used to represent the "metadata" partition.
 * The value 2_147_483_647 is reserved for the internal shift and cannot be used as a partition.
 * This class internally shifts the partitions by +1 to fit into the signed int range [0, 2_147_483_647].
 */
public class Partitions implements IntIterable, Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private static final int DELTA = -1;
    private static final int MAX_PARTITION = Integer.MAX_VALUE + DELTA;
    private static final int MIN_PARTITION = DELTA;
    private static final int DEFAULT_MAX_PARTITIONS = 128;
    private static final Partitions EMPTY = new Partitions(IntSortedSets.EMPTY_SET);
    private final IntSortedSet delegate;

    public Partitions() {
        this(DEFAULT_MAX_PARTITIONS);
    }

    public Partitions(int initialMaxPartition) {
        this(new IntSortedBitSet(encodePartition(initialMaxPartition)));
    }

    public Partitions(Set<Integer> initialValues) {
        this();
        initialValues.forEach(this::add);
    }

    public Partitions(IntSet initialValues) {
        this();
        initialValues.forEach(this::add);
    }

    public Partitions(Partitions initialValues) {
        this();
        addAll(initialValues);
    }

    private Partitions(IntSortedSet delegate) {
        this.delegate = delegate;
    }

    /**
     * Returns an unmodifiable empty Partitions instance.
     */
    public static Partitions empty() {
        return EMPTY;
    }

    public static Partitions singleton(int partition) {
        return new Partitions(IntSortedSets.singleton(encodePartition(partition)));
    }

    public static Collector<? super Integer, Partitions, Partitions> collector() {
        return Collector.of(Partitions::new, Partitions::add, (l, r) -> {
            l.addAll(r);
            return l;
        }, Collector.Characteristics.UNORDERED, Collector.Characteristics.IDENTITY_FINISH);
    }

    /**
     * Adds a partition to the set. Partition must be in the range [-1, 2_147_483_646].
     * @param partition the partition to add
     * @return true if the partition was added, false if it was already present
     * @throws IllegalArgumentException if the partition is out of range
     */
    public boolean add(int partition) {
        checkRange(partition);
        return delegate.add(encodePartition(partition));
    }

    /**
     * Removes a partition from the set. Partition must be in the range [-1, 2_147_483_646].
     * @param partition the partition to remove
     * @return true if the partition was added, false if it was already present
     * @throws IllegalArgumentException if the partition is out of range
     */
    public boolean remove(int partition) {
        checkRange(partition);
        return delegate.remove(encodePartition(partition));
    }

    private static void checkRange(int partition) {
        if (partition > MAX_PARTITION || partition < MIN_PARTITION) {
            throw new IllegalArgumentException("Partition number " + partition + " out of range");
        }
    }

    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    public void clear() {
        delegate.clear();
    }

    public boolean addAll(Set<Integer> activePartitions) {
        MutableBoolean retval = new MutableBoolean();
        activePartitions.forEach(p -> {
            if (add(p)) {
                retval.setTrue();
            }
        });
        return retval.booleanValue();
    }

    public boolean addAll(Partitions activePartitions) {
        return delegate.addAll(activePartitions.delegate);
    }

    public int size() {
        return delegate.size();
    }

    public boolean contains(int partition) {
        if (partition > MAX_PARTITION || partition < MIN_PARTITION) {
            return false;
        }
        return delegate.contains(encodePartition(partition));
    }

    @Override
    public IntIterator iterator() {
        return new IntIterator() {
            private final IntIterator delegateIter = delegate.iterator();

            @Override
            public boolean hasNext() {
                return delegateIter.hasNext();
            }

            @Override
            public int nextInt() {
                return decodePartition(delegateIter.nextInt());
            }
        };
    }

    public int getMinPartition() {
        return decodePartition(delegate.getFirst());
    }

    public int getMaxPartition() {
        return decodePartition(delegate.getLast());
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
            builder.append(DELTA);
            iter.nextInt(); // this is the zero we just printed
            if (iter.hasNext()) {
                builder.append(',');
            }
        }
        if (iter.hasNext()) {
            IntUtil.appendCompact(iter, builder, DELTA);
        }
        builder.append(']');
        return builder.toString();
    }

    public void removeAll(Partitions masterPartitions) {
        delegate.removeAll(masterPartitions.delegate);
    }

    public Partitions unmodifiable() {
        return new Partitions(IntSortedSets.unmodifiable(delegate));
    }

    public IntStream stream() {
        return delegate.intStream().map(Partitions::decodePartition);
    }

    private static int encodePartition(int partition) {
        return partition - DELTA;
    }

    private static int decodePartition(int value) {
        return value + DELTA;
    }
}
