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

package org.apache.hyracks.dataflow.std.structures;

public interface IMinHeap<T> extends IHeap<T> {
    /**
     * Removes and returns the smallest element in the tree.
     * Make sure the heap is not empty (by {@link #isEmpty()}) before calling this method
     *
     * @param result
     */
    void getMin(T result);

    /**
     * Returns (and does NOT remove) the smallest element in the tree
     *
     * @param result is the object that will eventually contain minimum entry
     *               pointer
     */
    void peekMin(T result);

    /**
     * Removes the current min and insert a new element.
     * Normally it is a faster way to call getMin() && insert() together
     *
     * @param newElement
     */
    void replaceMin(T newElement);
}
