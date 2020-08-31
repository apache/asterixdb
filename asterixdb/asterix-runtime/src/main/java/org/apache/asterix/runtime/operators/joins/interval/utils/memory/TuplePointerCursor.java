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
package org.apache.asterix.runtime.operators.joins.interval.utils.memory;

import java.util.Iterator;

import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class TuplePointerCursor extends AbstractTupleCursor<Iterator<TuplePointer>> {

    Iterator<TuplePointer> iterator;
    TuplePointer tp;

    public TuplePointerCursor(ITuplePointerAccessor accessor) {
        this.accessor = accessor;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public void next() {
        TuplePointer tp = iterator.next();
        this.tp = tp;
        tupleId = tp.getTupleIndex();
        ((ITuplePointerAccessor) accessor).reset(tp);
    }

    @Override
    public void reset(Iterator<TuplePointer> iterator) {
        this.iterator = iterator;
        tupleId = INITIALIZED;
    }

    public void remove() {
        iterator.remove();
    }

    public TuplePointer getTuplePointer() {
        return tp;
    }
}
