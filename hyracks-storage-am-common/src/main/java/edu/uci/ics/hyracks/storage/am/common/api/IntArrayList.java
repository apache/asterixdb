/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.common.api;

public class IntArrayList {
    private int[] data;
    private int size;
    private final int growth;

    public IntArrayList(int initialCapacity, int growth) {
        data = new int[initialCapacity];
        size = 0;
        this.growth = growth;
    }

    public int size() {
        return size;
    }

    public void add(int i) {
        if (size == data.length) {
            int[] newData = new int[data.length + growth];
            System.arraycopy(data, 0, newData, 0, data.length);
            data = newData;
        }

        data[size++] = i;
    }

    public void removeLast() {
        if (size > 0)
            size--;
    }

    // WARNING: caller is responsible for checking size > 0
    public int getLast() {
        return data[size - 1];
    }

    public int get(int i) {
        return data[i];
    }

    public void clear() {
        size = 0;
    }

    public boolean isEmpty() {
        return size == 0;
    }
}
