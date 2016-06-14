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

package org.apache.hyracks.storage.am.rtree.impls;

import java.util.Arrays;
import java.util.Collections;

public class TupleEntryArrayList {
    private TupleEntry[] data;
    private int size;
    private final int growth;

    public TupleEntryArrayList(int initialCapacity, int growth) {
        data = new TupleEntry[initialCapacity];
        size = 0;
        this.growth = growth;
    }

    public int size() {
        return size;
    }

    public void add(int tupleIndex, double value) {
        if (size == data.length) {
            TupleEntry[] newData = new TupleEntry[data.length + growth];
            System.arraycopy(data, 0, newData, 0, data.length);
            data = newData;
        }
        if (data[size] == null) {
            data[size] = new TupleEntry();
        }
        data[size].setTupleIndex(tupleIndex);
        data[size].setValue(value);
        size++;
    }

    public void removeLast() {
        if (size > 0)
            size--;
    }

    // WARNING: caller is responsible for checking size > 0
    public TupleEntry getLast() {
        return data[size - 1];
    }

    public TupleEntry get(int i) {
        return data[i];
    }

    public void clear() {
        size = 0;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public void sort(EntriesOrder order, int tupleCount) {
        if (order == EntriesOrder.ASCENDING) {
            Arrays.sort(data, 0, tupleCount);
        } else {
            Arrays.sort(data, 0, tupleCount, Collections.reverseOrder());
        }
    }
}
