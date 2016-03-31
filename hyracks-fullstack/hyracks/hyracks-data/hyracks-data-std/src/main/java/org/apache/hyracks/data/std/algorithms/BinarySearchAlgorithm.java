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
package org.apache.hyracks.data.std.algorithms;

import org.apache.hyracks.data.std.api.IComparable;
import org.apache.hyracks.data.std.collections.api.IValueReferenceVector;

/**
 * Performs Binary Search over a vector of value references.
 * Assumption: The items in the vector are sorted in ascending order with respect
 * to the specified key.
 *
 * @author vinayakb
 */
public class BinarySearchAlgorithm {
    private int index;

    public BinarySearchAlgorithm() {
    }

    /**
     * Perform binary search on the given vector for the specified key. It returns the result of the search
     * and sets the {@link #index} value that is retrievable by calling {@link #getIndex()} to refer to
     * the index in the array where the key was found (if it was found), and where it should have been (if
     * not found).
     *
     * @param vector
     *            - Sorted vector of items
     * @param key
     *            - Search key
     * @return <code>true</code> if the key is found, <code>false</code> otherwise.
     */
    public boolean find(IValueReferenceVector vector, IComparable key) {
        index = 0;
        int left = 0;
        int right = vector.getSize() - 1;
        while (left <= right) {
            index = (left + right) / 2;
            int cmp = key.compareTo(vector.getBytes(index), vector.getStart(index), vector.getLength(index));
            if (cmp > 0) {
                left = index + 1;
                index = left;
            } else if (cmp < 0) {
                right = index - 1;
                index = left;
            } else {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the index as a result of binary search.
     *
     * @return the index in the array where the key was found (if it was found), and where it should have been (if
     *         not found).
     */
    public int getIndex() {
        return index;
    }
}
