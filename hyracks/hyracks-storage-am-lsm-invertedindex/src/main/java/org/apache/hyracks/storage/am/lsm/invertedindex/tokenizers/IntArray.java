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

package org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers;

import java.util.Arrays;

public class IntArray {
    private static final int SIZE = 128;

    private int[] data;
    private int length;

    public IntArray() {
        data = new int[SIZE];
        length = 0;
    }

    public void add(int d) {
        if (length == data.length) {
            data = Arrays.copyOf(data, data.length << 1);
        }
        data[length++] = d;
    }

    public int[] get() {
        return data;
    }

    public int get(int i) {
        return data[i];
    }

    public int length() {
        return length;
    }

    public void reset() {
        length = 0;
    }

    public void sort() {
        sort(0, length);
    }

    public void sort(int start, int end) {
        Arrays.sort(data, start, end);
    }

    @Override
    public String toString() {
        StringBuilder out = new StringBuilder();
        out.append('[');
        for (int i = 0; i < length; ++i) {
            out.append(data[i]);
            if (i < length - 1) {
                out.append(',');
                out.append(' ');
            }
        }
        out.append(']');
        return out.toString();
    }
}
