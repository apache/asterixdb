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

package org.apache.hyracks.storage.am.btree.impls;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.storage.common.MultiComparator;

public class BatchPredicate extends RangePredicate {

    private static final long serialVersionUID = 1L;

    protected final FrameTupleReference keyTuple;
    protected final FrameTupleReference minFilterTuple;
    protected final FrameTupleReference maxFilterTuple;

    protected int keyIndex = -1;
    protected IFrameTupleAccessor accessor;

    public BatchPredicate(IFrameTupleAccessor accessor, MultiComparator keyCmp, int[] keyFields,
            int[] minFilterKeyFields, int[] maxFieldKeyFields) {
        super(null, null, true, true, keyCmp, keyCmp);
        this.keyIndex = 0;
        this.accessor = accessor;
        if (keyFields != null && keyFields.length > 0) {
            this.keyTuple = new PermutingFrameTupleReference(keyFields);
        } else {
            this.keyTuple = new FrameTupleReference();
        }
        if (minFilterKeyFields != null && minFilterKeyFields.length > 0) {
            this.minFilterTuple = new PermutingFrameTupleReference(minFilterKeyFields);
        } else {
            this.minFilterTuple = null;
        }
        if (maxFieldKeyFields != null && maxFieldKeyFields.length > 0) {
            this.maxFilterTuple = new PermutingFrameTupleReference(maxFieldKeyFields);
        } else {
            this.maxFilterTuple = null;
        }

    }

    public void reset(IFrameTupleAccessor accessor) {
        this.keyIndex = -1;
        this.accessor = accessor;
    }

    private boolean isValid() {
        return accessor != null && keyIndex >= 0 && keyIndex < accessor.getTupleCount();
    }

    @Override
    public ITupleReference getLowKey() {
        return isValid() ? keyTuple : null;
    }

    @Override
    public ITupleReference getHighKey() {
        return isValid() ? keyTuple : null;
    }

    @Override
    public ITupleReference getMinFilterTuple() {
        return isValid() ? minFilterTuple : null;
    }

    @Override
    public ITupleReference getMaxFilterTuple() {
        return isValid() ? maxFilterTuple : null;
    }

    @Override
    public boolean isPointPredicate(MultiComparator originalKeyComparator) throws HyracksDataException {
        return true;
    }

    public boolean hasNext() {
        return accessor != null && keyIndex < accessor.getTupleCount() - 1;
    }

    public void next() {
        keyIndex++;
        if (isValid()) {
            keyTuple.reset(accessor, keyIndex);
            if (minFilterTuple != null) {
                minFilterTuple.reset(accessor, keyIndex);
            }
            if (maxFilterTuple != null) {
                maxFilterTuple.reset(accessor, keyIndex);
            }
        }
    }

    public int getKeyIndex() {
        return keyIndex;
    }

    public int getNumKeys() {
        return accessor.getTupleCount();
    }

}
