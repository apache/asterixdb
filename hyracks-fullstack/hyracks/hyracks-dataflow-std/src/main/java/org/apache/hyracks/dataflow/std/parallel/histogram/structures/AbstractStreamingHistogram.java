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

package org.apache.hyracks.dataflow.std.parallel.histogram.structures;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.dataflow.std.parallel.IDTHistogram;
import org.apache.hyracks.dataflow.std.parallel.IHistogram;

/**
 * @author michael
 */
public class AbstractStreamingHistogram<E extends AbstractPointable> implements IDTHistogram<E> {
    //        GrowableArray quantiles;
    private static final int DEFAULT_WINDOW_FACTOR = 10;
    private final IBinaryComparator[] comparators;
    private final List<Entry<E, Integer>> quantiles;
    private final List<E> buffered;
    protected final double mu;
    private final int elastic;
    private final int buckets;
    private final int blocked;
    private final int windows;
    protected final int granulars;
    protected final int threshold;

    private int current = 0;
    private int bucket = 0;

    public class Quantile<K, V> implements Entry<K, V> {
        private K key;
        private V value;

        public Quantile(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            V old = this.value;
            this.value = value;
            return old;
        }
    }

    public AbstractStreamingHistogram(IBinaryComparator[] comparators, int nu, int el, double mu) {
        this.comparators = comparators;
        this.mu = mu;
        this.buckets = nu;
        this.elastic = el;
        this.blocked = elastic * buckets;
        this.windows = blocked * DEFAULT_WINDOW_FACTOR;
        this.threshold = (int) Math.floor((windows / blocked) * mu);
        this.granulars = windows / blocked;
        this.quantiles = new ArrayList<Entry<E, Integer>>(blocked);
        this.buffered = new ArrayList<E>();
    }

    private int compareKey(E left, E right) throws HyracksDataException {
        byte[] leftBuf = ((IPointable) left).getByteArray();
        byte[] rightBuf = ((IPointable) right).getByteArray();
        int ret = 0;
        for (int i = 0; i < comparators.length; i++) {
            ret = comparators[i].compare(leftBuf, 0, leftBuf.length, rightBuf, 0, rightBuf.length);
            if (ret != 0)
                break;
        }
        return 0;
    }

    private void sortBuffered() {
        Collections.sort(buffered, new Comparator<E>() {
            @Override
            public int compare(E q1, E q2) {
                try {
                    return compareKey(q1, q2);
                } catch (HyracksDataException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                return Integer.MIN_VALUE;
            }
        });
    }

    public void sortQuantiles() {
        Collections.sort(quantiles, new Comparator<Entry<E, Integer>>() {
            @Override
            public int compare(Entry<E, Integer> q1, Entry<E, Integer> q2) {
                try {
                    return compareKey(q1.getKey(), q2.getKey());
                } catch (HyracksDataException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                return Integer.MIN_VALUE;
            }
        });
    }

    public int binarySearch(E next) throws HyracksDataException {
        int low = 0;
        int high = bucket;
        while (high - low != 1) {
            int middle = (high - low) / 2 + low;
            if (compareKey(quantiles.get(middle).getKey(), next) > 0)
                high = middle;
            else
                low = middle;
        }
        return low;
    }

    public void updateIncreaseQuantile(int at, E quantile) {
        quantiles.set(at, new Quantile<E, Integer>(quantile, quantiles.get(at).getValue() + 1));
    }

    @Override
    public E mediate(E left, E right) {
        /*byte[] bm = new byte[left.length];
        double dm = (DoublePointable.getDouble(left, 0) + DoublePointable.getDouble(right, 0)) / 2;
        DoublePointable.setDouble(bm, 0, dm);
        return bm;*/
        return null;
    }

    /*private int comparator(IHistogram left, IHistogram right) {
        return 0;
    }

    private void merge() {

    }

    private void split() {

    }*/

    @Override
    public int getCurrent() {
        return current;
    }

    @Override
    public void merge(IHistogram<E> ba) throws HyracksDataException {
        /*int total = current + ba.getCurrent();
        current = 0;*/
    }

    @Override
    public void addItem(E item) {
        int diff = ++current - windows;
        if (diff > 0) {
            //Need to insert into quantiles;
        } else if (diff == 0) {
            //Need to initial quantiles in batch
            sortBuffered();
        } else {
            //Just insert into buffer
            this.buffered.add(item);
        }
    }

    @Override
    public void countItem(E item) {

    }

    @Override
    public List<Entry<E, Integer>> generate(boolean isGlobal) {
        return quantiles;
    }

    @Override
    public void initialize() {
        this.quantiles.clear();
        this.buffered.clear();
    }

    @Override
    public void countReset() {

    }

    @Override
    public FieldType getType() {
        // TODO Auto-generated method stub
        return null;
    }
}
