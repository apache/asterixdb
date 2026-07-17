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

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.common.MultiComparator;

public class BatchPredicateWithKeys extends RangePredicate {
    private static final long serialVersionUID = 1L;

    protected List<ITupleReference> keyTuples;
    protected ITupleReference keyTuple;
    private int keyIndex;

    public BatchPredicateWithKeys() {
        this.keyIndex = -1;
    }

    public void reset(List<ITupleReference> keyTuples) {
        this.keyTuples = keyTuples;
        this.keyIndex = -1;
    }

    private boolean isValid() {
        return keyIndex >= 0 && keyIndex < keyTuples.size();
    }

    @Override
    public ITupleReference getLowKey() {
        return isValid() ? keyTuples.get(keyIndex) : null;
    }

    @Override
    public ITupleReference getHighKey() {
        return isValid() ? keyTuples.get(keyIndex) : null;
    }

    @Override
    public ITupleReference getMinFilterTuple() {
        return null;
    }

    @Override
    public ITupleReference getMaxFilterTuple() {
        return null;
    }

    @Override
    public boolean isPointPredicate(MultiComparator originalKeyComparator) throws HyracksDataException {
        return true;
    }

    public boolean hasNext() {
        return keyIndex + 1 < keyTuples.size();
    }

    public void next() {
        keyIndex++;
        if (isValid()) {
            keyTuple = keyTuples.get(keyIndex);
        }
    }

    // use this to remove the keys from this index, as this index is present in the later components.
    public int getKeyIndex() {
        return keyIndex;
    }

    public int getNumKeys() {
        return keyTuples.size();
    }
}
