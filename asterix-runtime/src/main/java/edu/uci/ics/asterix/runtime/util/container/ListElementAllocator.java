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

/**
 * ListElementAllocator<E, T> is an element-reusable list or a element pool in other words, however 
 * elements in the list should be exactly the same class, this is forced by IElementFactory<E, T> factory 
 * as a parameter to the constructor once a ListElementAllocator is constructed, it can only store 
 * objects of the same class
 */
public class ListElementAllocator<E, T> implements IElementAllocator<E, T> {

    private IElementFactory<E, T> factory;
    private List<E> pool = new ArrayList<E>();
    private int cursor = -1;

    public ListElementAllocator(IElementFactory<E, T> factory) {
        this.factory = factory;
    }

    @Override
    public E allocate(T arg) {
        cursor++;
        if (cursor < pool.size()) {
            return pool.get(cursor);
        } else {
            E element = factory.createElement(arg);
            pool.add(element);
            return element;
        }
    }

    @Override
    public void reset() {
        cursor = -1;
    }
}
