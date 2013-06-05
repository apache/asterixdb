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

package edu.uci.ics.asterix.om.util.container;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Object pool backed by a list.
 * 
 * The argument for creating E instances could be different. This class also
 * considers arguments in object reusing, e.g., it reuses an E instances ONLY
 * when the construction argument is "equal".
 */
public class ListObjectPool<E, T> implements IObjectPool<E, T> {

    private IObjectFactory<E, T> factory;

    /**
     * cache of objects
     */
    private List<E> pool = new ArrayList<E>();

    /**
     * args that are used to create each element in the pool
     */
    private List<T> args = new ArrayList<T>();

    /**
     * bits indicating which element is in use
     */
    private BitSet usedBits = new BitSet();

    public ListObjectPool(IObjectFactory<E, T> factory) {
        this.factory = factory;
    }

    @Override
    public E allocate(T arg) {
        int freeSlot = -1;
        while (freeSlot + 1 < pool.size()) {
            freeSlot = usedBits.nextClearBit(freeSlot + 1);
            if (freeSlot >= pool.size())
                break;

            // the two cases where an element in the pool is a match
            if ((arg == null && args.get(freeSlot) == null)
                    || (arg != null && args.get(freeSlot) != null && arg.equals(args.get(freeSlot)))) {
                // the element is not used and the arg is the same as
                // input arg
                usedBits.set(freeSlot);
                return pool.get(freeSlot);
            }
        }

        // if we do not find a reusable object, allocate a new one
        E element = factory.create(arg);
        pool.add(element);
        args.add(arg);
        usedBits.set(pool.size() - 1);
        return element;
    }

    @Override
    public void reset() {
        usedBits.clear();
    }
}
