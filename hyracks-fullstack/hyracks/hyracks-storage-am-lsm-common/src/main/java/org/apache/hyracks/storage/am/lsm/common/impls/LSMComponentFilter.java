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
package org.apache.hyracks.storage.am.lsm.common.impls;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.common.MultiComparator;

public class LSMComponentFilter implements ILSMComponentFilter {

    private final IBinaryComparatorFactory[] filterCmpFactories;
    private final ITreeIndexTupleWriter tupleWriter;

    private ITupleReference minTuple;
    private ITupleReference maxTuple;

    private byte[] minTupleBytes;
    private ByteBuffer minTupleBuf;

    private byte[] maxTupleBytes;
    private ByteBuffer maxTupleBuf;

    public LSMComponentFilter(ITreeIndexTupleWriter tupleWriter, IBinaryComparatorFactory[] filterCmpFactories) {
        this.filterCmpFactories = filterCmpFactories;
        this.tupleWriter = tupleWriter;
    }

    @Override
    public IBinaryComparatorFactory[] getFilterCmpFactories() {
        return filterCmpFactories;
    }

    @Override
    public void reset() {
        minTuple = null;
        maxTuple = null;
        minTupleBytes = null;
        maxTupleBytes = null;
        minTupleBuf = null;
        maxTupleBuf = null;
    }

    @Override
    public void update(ITupleReference tuple, MultiComparator cmp, IExtendedModificationOperationCallback opCallback)
            throws HyracksDataException {
        boolean logged = false;
        if (minTuple == null) {
            int numBytes = tupleWriter.bytesRequired(tuple);
            minTupleBytes = new byte[numBytes];
            opCallback.after(tuple);
            logged = true;
            tupleWriter.writeTuple(tuple, minTupleBytes, 0);
            minTupleBuf = ByteBuffer.wrap(minTupleBytes);
            minTuple = tupleWriter.createTupleReference();
            ((ITreeIndexTupleReference) minTuple).resetByTupleOffset(minTupleBuf.array(), 0);
        } else {
            int c = cmp.compare(tuple, minTuple);
            if (c < 0) {
                opCallback.after(tuple);
                logged = true;
                int numBytes = tupleWriter.bytesRequired(tuple);
                if (minTupleBytes.length < numBytes) {
                    minTupleBytes = new byte[numBytes];
                    tupleWriter.writeTuple(tuple, minTupleBytes, 0);
                    minTupleBuf = ByteBuffer.wrap(minTupleBytes);
                } else {
                    tupleWriter.writeTuple(tuple, minTupleBytes, 0);
                }
                ((ITreeIndexTupleReference) minTuple).resetByTupleOffset(minTupleBuf.array(), 0);
            }
        }
        if (maxTuple == null) {
            int numBytes = tupleWriter.bytesRequired(tuple);
            maxTupleBytes = new byte[numBytes];
            if (!logged) {
                opCallback.after(tuple);
            }
            tupleWriter.writeTuple(tuple, maxTupleBytes, 0);
            maxTupleBuf = ByteBuffer.wrap(maxTupleBytes);
            maxTuple = tupleWriter.createTupleReference();
            ((ITreeIndexTupleReference) maxTuple).resetByTupleOffset(maxTupleBuf.array(), 0);
        } else {
            int c = cmp.compare(tuple, maxTuple);
            if (c > 0) {
                if (!logged) {
                    opCallback.after(tuple);
                }
                int numBytes = tupleWriter.bytesRequired(tuple);
                if (maxTupleBytes.length < numBytes) {
                    maxTupleBytes = new byte[numBytes];
                    tupleWriter.writeTuple(tuple, maxTupleBytes, 0);
                    maxTupleBuf = ByteBuffer.wrap(maxTupleBytes);
                } else {
                    tupleWriter.writeTuple(tuple, maxTupleBytes, 0);
                }
                ((ITreeIndexTupleReference) maxTuple).resetByTupleOffset(maxTupleBuf.array(), 0);
            }
        }
    }

    @Override
    public ITupleReference getMinTuple() {
        return minTuple;
    }

    @Override
    public ITupleReference getMaxTuple() {
        return maxTuple;
    }

    @Override
    public boolean satisfy(ITupleReference minTuple, ITupleReference maxTuple, MultiComparator filterCmp)
            throws HyracksDataException {
        if (maxTuple != null && this.minTuple != null) {
            int c = filterCmp.compare(maxTuple, this.minTuple);
            if (c < 0) {
                return false;
            }
        }
        if (minTuple != null && this.maxTuple != null) {
            int c = filterCmp.compare(minTuple, this.maxTuple);
            if (c > 0) {
                return false;
            }
        }
        return true;
    }

}
