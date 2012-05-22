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

public class ListElementAllocator<E, T> implements IElementAllocator<E, T> {

    private IElementFactory<E, T> factory;
    private List<E> pool = new ArrayList<E>();
    private int cursor = -1;

    public ListElementAllocator(IElementFactory<E, T> factory) {
        this.factory = factory;
    }

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

    public void reset() {
        cursor = -1;
    }
}
