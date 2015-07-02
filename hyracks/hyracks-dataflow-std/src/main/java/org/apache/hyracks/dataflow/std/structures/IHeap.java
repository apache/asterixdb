/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hyracks.dataflow.std.structures;

public interface IHeap<T> {
    /**
     * Inserts a new element into the selectionTree
     *
     * @param element to be inserted
     */
    void insert(T element);

    /**
     * @return True of the selection tree does not have any element, false
     * otherwise
     */
    boolean isEmpty();

    /**
     * Removes all the elements in the tree
     */
    void reset();

    /**
     * Return the number of the inserted tuples
     *
     * @return
     */
    int getNumEntries();

}
