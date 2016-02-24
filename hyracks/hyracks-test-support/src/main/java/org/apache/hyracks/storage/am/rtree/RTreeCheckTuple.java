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

package org.apache.hyracks.storage.am.rtree;

import org.apache.hyracks.storage.am.common.CheckTuple;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class RTreeCheckTuple<T> extends CheckTuple {

    public RTreeCheckTuple(int numFields, int numKeys) {
        super(numFields, numKeys);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof RTreeCheckTuple) {
            RTreeCheckTuple<T> other = (RTreeCheckTuple<T>) o;
            for (int i = 0; i < fields.length; i++) {
                int cmp = fields[i].compareTo(other.getField(i));
                if (cmp != 0) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    public boolean intersect(T o) {
        RTreeCheckTuple<T> other = (RTreeCheckTuple<T>) o;
        int maxFieldPos = numKeys / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int cmp = fields[i].compareTo(other.getField(j));
            if (cmp > 0) {
                return false;
            }
            cmp = fields[j].compareTo(other.getField(i));
            if (cmp < 0) {
                return false;
            }
        }
        return true;
    }

}
