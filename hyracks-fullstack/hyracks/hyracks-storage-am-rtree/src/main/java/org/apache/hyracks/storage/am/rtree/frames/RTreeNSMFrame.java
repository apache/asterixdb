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

package org.apache.hyracks.storage.am.rtree.frames;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProvider;
import org.apache.hyracks.storage.am.common.api.ISplitKey;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.frames.TreeIndexNSMFrame;
import org.apache.hyracks.storage.am.rtree.api.IRTreeFrame;
import org.apache.hyracks.storage.am.rtree.api.IRTreePolicy;
import org.apache.hyracks.storage.am.rtree.impls.UnorderedSlotManager;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;

public abstract class RTreeNSMFrame extends TreeIndexNSMFrame implements IRTreeFrame {
    protected static final int PAGE_NSN_OFFSET = TreeIndexNSMFrame.RESERVED_HEADER_SIZE;
    protected static final int RIGHT_PAGE_OFFSET = PAGE_NSN_OFFSET + 8;

    protected ITreeIndexTupleReference[] mbrTuples;
    protected ITreeIndexTupleReference cmpFrameTuple;

    private static final double DOUBLE_EPSILON = computeDoubleEpsilon();
    protected final IPrimitiveValueProvider[] keyValueProviders;

    protected IRTreePolicy rtreePolicy;
    protected final boolean isPointMBR;

    public RTreeNSMFrame(ITreeIndexTupleWriter tupleWriter, IPrimitiveValueProvider[] keyValueProviders,
            RTreePolicyType rtreePolicyType, boolean isPointMBR) {
        super(tupleWriter, new UnorderedSlotManager());
        this.mbrTuples = new ITreeIndexTupleReference[keyValueProviders.length];
        for (int i = 0; i < keyValueProviders.length; i++) {
            this.mbrTuples[i] = tupleWriter.createTupleReference();
        }
        cmpFrameTuple = tupleWriter.createTupleReference();
        this.keyValueProviders = keyValueProviders;

        if (rtreePolicyType == RTreePolicyType.RTREE) {
            rtreePolicy = new RTreePolicy(tupleWriter, keyValueProviders, cmpFrameTuple, TOTAL_FREE_SPACE_OFFSET);
        } else {
            rtreePolicy = new RStarTreePolicy(tupleWriter, keyValueProviders, cmpFrameTuple, TOTAL_FREE_SPACE_OFFSET);
        }
        this.isPointMBR = isPointMBR;
    }

    private static double computeDoubleEpsilon() {
        double doubleEpsilon = 1.0;

        do {
            doubleEpsilon /= 2.0;
        } while (1.0 + (doubleEpsilon / 2.0) != 1.0);
        return doubleEpsilon;
    }

    public static double doubleEpsilon() {
        return DOUBLE_EPSILON;
    }

    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        buf.putLong(PAGE_NSN_OFFSET, 0);
        buf.putInt(RIGHT_PAGE_OFFSET, -1);
    }

    public void setTupleCount(int tupleCount) {
        buf.putInt(Constants.TUPLE_COUNT_OFFSET, tupleCount);
    }

    @Override
    public void setPageNsn(long pageNsn) {
        buf.putLong(PAGE_NSN_OFFSET, pageNsn);
    }

    @Override
    public long getPageNsn() {
        return buf.getLong(PAGE_NSN_OFFSET);
    }

    @Override
    public int getRightPage() {
        return buf.getInt(RIGHT_PAGE_OFFSET);
    }

    @Override
    public void setRightPage(int rightPage) {
        buf.putInt(RIGHT_PAGE_OFFSET, rightPage);
    }

    public ITreeIndexTupleReference[] getMBRTuples() {
        return mbrTuples;
    }

    @Override
    public void split(ITreeIndexFrame rightFrame, ITupleReference tuple, ISplitKey splitKey,
            IExtraPageBlockHelper extraPageBlockHelper, IBufferCache bufferCache) throws HyracksDataException {
        rtreePolicy.split(this, buf, rightFrame, slotManager, frameTuple, tuple, splitKey);
    }

    abstract public int getTupleSize(ITupleReference tuple);

    protected void calculateMBRImpl(ITreeIndexTupleReference[] tuples) {
        int maxFieldPos = keyValueProviders.length / 2;
        for (int i = 1; i < getTupleCount(); i++) {
            frameTuple.resetByTupleIndex(this, i);
            for (int j = 0; j < maxFieldPos; j++) {
                int k = maxFieldPos + j;
                double valA = keyValueProviders[j].getValue(frameTuple.getFieldData(j), frameTuple.getFieldStart(j));
                double valB = keyValueProviders[j].getValue(tuples[j].getFieldData(j), tuples[j].getFieldStart(j));
                if (valA < valB) {
                    tuples[j].resetByTupleIndex(this, i);
                }
                valA = keyValueProviders[k].getValue(frameTuple.getFieldData(k), frameTuple.getFieldStart(k));
                valB = keyValueProviders[k].getValue(tuples[k].getFieldData(k), tuples[k].getFieldStart(k));
                if (valA > valB) {
                    tuples[k].resetByTupleIndex(this, i);
                }
            }
        }
    }

    @Override
    public void adjustMBR() {
        for (int i = 0; i < mbrTuples.length; i++) {
            mbrTuples[i].setFieldCount(getFieldCount());
            mbrTuples[i].resetByTupleIndex(this, 0);
        }

        calculateMBRImpl(mbrTuples);
    }

    public abstract int getFieldCount();

    @Override
    public int getPageHeaderSize() {
        return RIGHT_PAGE_OFFSET + 4;
    }

    @Override
    public void setMultiComparator(MultiComparator cmp) {
        // currently, R-Tree Frames are unsorted
    }

    @Override
    public String toString() {
        return new StringBuilder(this.getClass().getSimpleName()).append('\n').append("Tuple Count: " + getTupleCount())
                .append('\n').append("Free Space offset: " + buf.getInt(Constants.FREE_SPACE_OFFSET)).append('\n')
                .append("Level: " + buf.get(Constants.LEVEL_OFFSET)).append('\n')
                .append("LSN: " + buf.getLong(PAGE_LSN_OFFSET)).append('\n')
                .append("Total Free Space: " + buf.getInt(TOTAL_FREE_SPACE_OFFSET)).append('\n')
                .append("Flag: " + buf.get(FLAG_OFFSET)).append('\n').append("NSN: " + buf.getLong(PAGE_NSN_OFFSET))
                .append('\n').append("Right Page:").append(buf.getInt(RIGHT_PAGE_OFFSET)).toString();
    }
}
