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

package edu.uci.ics.hyracks.storage.am.rtree.tests;

import edu.uci.ics.hyracks.storage.am.common.test.CheckTuple;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class RTreeCheckTuple<T> extends CheckTuple {

    public RTreeCheckTuple(int numFields, int numKeys) {
        super(numFields, numKeys);
    }

    @Override
    public boolean equals(Object o) {
        RTreeCheckTuple<T> other = (RTreeCheckTuple<T>) o;
        for (int i = 0; i < tuple.length; i++) {
            int cmp = tuple[i].compareTo(other.get(i));
            if (cmp != 0) {
                return false;
            }
        }
        return true;
    }

    public boolean intersect(T o) {
        RTreeCheckTuple<T> other = (RTreeCheckTuple<T>) o;
        int maxFieldPos = numKeys / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int cmp = tuple[i].compareTo(other.get(j));
            if (cmp > 0) {
                return false;
            }
            cmp = tuple[j].compareTo(other.get(i));
            if (cmp < 0) {
                return false;
            }
        }
        return true;
    }

}