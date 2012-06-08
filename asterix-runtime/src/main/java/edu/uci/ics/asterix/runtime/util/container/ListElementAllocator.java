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

package edu.uci.ics.asterix.runtime.util.container;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BooleanWritable;

/**
 * ListElementAllocator<E, T> is an element-reusable list or a element pool in
 * other words, however elements in the list should be exactly the same class,
 * this is forced by IElementFactory<E, T> factory as a parameter to the
 * constructor once a ListElementAllocator is constructed, it can only store
 * objects of the same class
 */
public class ListElementAllocator<E, T> implements IElementAllocator<E, T> {

    private IElementFactory<E, T> factory;

    /**
     * element reusable object pool
     */
    private List<E> pool = new ArrayList<E>();

    /**
     * args that are used to create each element in the pool
     */
    private List<T> args = new ArrayList<T>();

    /**
     * bits indicating which element is in use
     */
    private List<BooleanWritable> usedBits = new ArrayList<BooleanWritable>();

    /**
     * the start index for searching
     */
    private int minStartIndex = 0;

    public ListElementAllocator(IElementFactory<E, T> factory) {
        this.factory = factory;
    }

    @Override
    public E allocate(T arg) {
        boolean continuous = true;
        for (int i = minStartIndex; i < pool.size(); i++) {
            if (!usedBits.get(i).get()) {
                boolean match = false;

                // the two cases where an element in the pool is a match
                if ((arg == null && args.get(i) == null)
                        || (arg != null && args.get(i) != null && arg.equals(args.get(i))))
                    match = true;

                if (match) {
                    // the element is not used and the arg is the same as input
                    // arg
                    if (continuous)
                        minStartIndex++;

                    usedBits.get(i).set(true);
                    return pool.get(i);
                } else {
                    // a unmatched element blocked
                    // free slots are not continuous from the beginning free
                    // slot
                    continuous = false;
                }
            } else {
                if (continuous)
                    minStartIndex++;
            }
        }

        // if not find a reusable object, allocate a new element
        E element = factory.createElement(arg);
        pool.add(element);
        args.add(arg);
        usedBits.add(new BooleanWritable(true));
        return element;
    }

    @Override
    public void reset() {
        for (int i = 0; i < usedBits.size(); i++)
            usedBits.get(i).set(false);
        minStartIndex = 0;
    }
}
