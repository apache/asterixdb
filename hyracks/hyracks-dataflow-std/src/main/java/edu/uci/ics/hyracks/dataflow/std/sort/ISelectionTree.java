/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.std.sort;

/**
 * @author pouria
 *         Defines the selection tree, used in sorting with replacement
 *         selection to manage the order of output tuples into the runs, during
 *         the run generation phase. This tree contains tuples, belonging to two
 *         different runs: - Current run (being written to the output) - Next
 *         run
 */

public interface ISelectionTree {

    /**
     * Inserts a new element into the selectionTree
     * 
     * @param element
     *            contains the pointer to the memory slot, containing the tuple,
     *            along with its run number
     */
    void insert(int[] element);

    /**
     * Removes and returns the smallest element in the tree
     * 
     * @param result
     *            is the array that will eventually contain minimum entry
     *            pointer
     */
    void getMin(int[] result);

    /**
     * Removes and returns the largest element in the tree
     * 
     * @param result
     *            is the array that will eventually contain maximum entry
     *            pointer
     */
    void getMax(int[] result);

    /**
     * @return True of the selection tree does not have any element, false
     *         otherwise
     */
    boolean isEmpty();

    /**
     * Removes all the elements in the tree
     */
    void reset();

    /**
     * Returns (and does NOT remove) the smallest element in the tree
     * 
     * @param result
     *            is the array that will eventually contain minimum entry
     *            pointer
     */
    void peekMin(int[] result);

    /**
     * Returns (and does NOT remove) the largest element in the tree
     * 
     * @param result
     *            is the array that will eventually contain maximum entry
     *            pointer
     */
    void peekMax(int[] result);

}