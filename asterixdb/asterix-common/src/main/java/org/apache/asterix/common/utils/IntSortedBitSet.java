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
import java.util.Arrays;
import java.util.NoSuchElementException;

import it.unimi.dsi.fastutil.ints.AbstractIntSortedSet;
import it.unimi.dsi.fastutil.ints.IntBidirectionalIterator;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntComparators;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

public class IntSortedBitSet extends AbstractIntSortedSet implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private static final int BITS_PER_ELEMENT = Long.SIZE;
    private long[] storage;

    public IntSortedBitSet(int initialMaxValue) {
        storage = new long[initialMaxValue / BITS_PER_ELEMENT + 1];
    }

    public IntSortedBitSet() {
        this(1);
    }

    public IntSortedBitSet(IntCollection initial) {
        this();
        addAll(initial);
    }

    public IntSortedBitSet(int[] initial) {
        this(Math.max(initial.length - 1, 1));
        for (int value : initial) {
            add(value);
        }
    }

    @Override
    public boolean contains(int k) {
        checkRange(k);
        int elementIndex = k / BITS_PER_ELEMENT;
        int bitIndex = k % BITS_PER_ELEMENT;
        return storage.length > elementIndex && (storage[elementIndex] & 1L << bitIndex) != 0;
    }

    private void checkRange(int k) {
        if (k < 0) {
            throw new IllegalArgumentException("negative values not supported");
        }
    }

    @Override
    public boolean add(int k) {
        checkRange(k);
        int elementIndex = k / BITS_PER_ELEMENT;
        int bitIndex = k % BITS_PER_ELEMENT;
        ensureCapacity(elementIndex);
        boolean absent = (storage[elementIndex] & 1L << bitIndex) == 0;
        storage[elementIndex] |= 1L << bitIndex;
        return absent;
    }

    @Override
    public boolean remove(int k) {
        checkRange(k);
        int elementIndex = k / BITS_PER_ELEMENT;
        int bitIndex = k % BITS_PER_ELEMENT;
        if (storage.length <= elementIndex) {
            return false;
        }
        boolean present = (storage[elementIndex] & 1L << bitIndex) != 0;
        storage[elementIndex] &= ~(1L << bitIndex);
        return present;
    }

    @Override
    public void clear() {
        Arrays.fill(storage, 0L);
    }

    @Override
    public boolean equals(Object o) {
        if (o.getClass() == getClass()) {
            return Arrays.equals(storage, ((IntSortedBitSet) o).storage);
        }
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(storage);
    }

    private void ensureCapacity(int index) {
        if (storage.length <= index) {
            long[] newStorage = new long[index + 1];
            System.arraycopy(storage, 0, newStorage, 0, storage.length);
            storage = newStorage;
        }
    }

    @Override
    public IntSortedSet subSet(int fromElement, int toElement) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public IntSortedSet headSet(int toElement) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public IntSortedSet tailSet(int fromElement) {
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public IntComparator comparator() {
        return IntComparators.NATURAL_COMPARATOR;
    }

    @Override
    public int firstInt() {
        for (int index = 0; index < storage.length; index++) {
            if (storage[index] != 0) {
                return (int) (index * BITS_PER_ELEMENT
                        + (int) MathUtil.log2Unsigned(Long.lowestOneBit(storage[index])));
            }
        }
        return -1;
    }

    @Override
    public int lastInt() {
        for (int index = storage.length - 1; index >= 0; index--) {
            if (storage[index] != 0) {
                return index * BITS_PER_ELEMENT + (int) MathUtil.log2Unsigned(Long.highestOneBit(storage[index]));
            }
        }
        return -1;
    }

    @Override
    public int size() {
        int size = 0;
        for (long element : storage) {
            size += Long.bitCount(element);
        }
        return size;
    }

    @Override
    public IntBidirectionalIterator iterator() {
        final int firstInt = firstInt();
        return iterator(firstInt, firstInt);
    }

    @Override
    public IntBidirectionalIterator iterator(int fromElement) {
        return iterator(firstInt(), fromElement);
    }

    @Override
    public String toString() {
        return IntUtil.toCompactString(iterator());
    }

    private IntBidirectionalIterator iterator(final int first, final int fromElement) {
        return new IntBidirectionalIterator() {
            final int last = lastInt();
            int position = fromElement;
            int lastReturned = -1;

            @Override
            public int previousInt() {
                for (; position >= first; position--) {
                    if ((storage[position / BITS_PER_ELEMENT] & 1L << (position % BITS_PER_ELEMENT)) != 0) {
                        lastReturned = position;
                        return position--;
                    }
                }
                throw new NoSuchElementException();
            }

            @Override
            public boolean hasPrevious() {
                return first >= 0 && position >= first;
            }

            @Override
            public int nextInt() {
                for (; position <= last; position++) {
                    if ((storage[position / BITS_PER_ELEMENT] & 1L << (position % BITS_PER_ELEMENT)) != 0) {
                        lastReturned = position;
                        return position++;
                    }
                }
                throw new NoSuchElementException();
            }

            @Override
            public boolean hasNext() {
                return last >= 0 && position <= last;
            }

            @Override
            public void remove() {
                IntSortedBitSet.this.remove(lastReturned);
            }
        };
    }

}
