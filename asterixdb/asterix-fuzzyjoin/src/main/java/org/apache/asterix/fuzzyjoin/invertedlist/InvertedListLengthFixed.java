/**
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

package org.apache.asterix.fuzzyjoin.invertedlist;

import java.util.Iterator;

public class InvertedListLengthFixed implements InvertedList {
    public class ListIterator implements Iterator<int[]> {

        int ix;

        public ListIterator(int ix) {
            this.ix = ix;
        }

        public boolean hasNext() {
            return ix < sz;
        }

        public int[] next() {
            return list[ix++];
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private final int[][] list;

    private int sz, ix;

    public InvertedListLengthFixed(int size) {
        list = new int[size * 3][3];
        sz = 0;
        ix = 0;
    }

    public void add(int[] element) {
        list[sz++] = element;
    }

    public int getIndex() {
        return ix;
    }

    public int getSize() {
        return sz;
    }

    public Iterator<int[]> iterator() {
        // return Arrays.asList(list).iterator();
        return new ListIterator(ix);
    }

    public void setMinLength(int minLength) {
        while (ix < sz && list[ix][2] < minLength) {
            ix++;
        }
    }
}
