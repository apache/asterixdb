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
package org.apache.hyracks.storage.am.lsm.vector.impls;

import org.apache.hyracks.dataflow.std.structures.IResetableComparable;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * Heap entry for the frame-backed top-K window in vector search cursors.
 * Keyed by D(q, x) (approximate distance to query) — largest dqx at the root
 * of a MaxHeap so the worst result can be evicted in O(1) peek / O(log n) replace.
 */
public class VectorSearchHeapEntry implements IResetableComparable<VectorSearchHeapEntry> {
    final TuplePointer tuplePointer;
    double dqx;

    public VectorSearchHeapEntry() {
        this.tuplePointer = new TuplePointer();
        this.dqx = Double.MAX_VALUE;
    }

    @Override
    public int compareTo(VectorSearchHeapEntry other) {
        return Double.compare(this.dqx, other.dqx);
    }

    @Override
    public void reset(VectorSearchHeapEntry other) {
        this.tuplePointer.reset(other.tuplePointer);
        this.dqx = other.dqx;
    }

    public void reset(TuplePointer pointer, double dqx) {
        this.tuplePointer.reset(pointer);
        this.dqx = dqx;
    }
}
