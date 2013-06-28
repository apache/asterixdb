/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.rtree.frames;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.frames.TreeIndexNSMFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreePolicy;
import edu.uci.ics.hyracks.storage.am.rtree.impls.UnorderedSlotManager;

public abstract class RTreeNSMFrame extends TreeIndexNSMFrame implements IRTreeFrame {
    protected static final int pageNsnOff = smFlagOff + 1;
    protected static final int rightPageOff = pageNsnOff + 8;

    protected ITreeIndexTupleReference[] tuples;
    protected ITreeIndexTupleReference cmpFrameTuple;

    private static final double doubleEpsilon = computeDoubleEpsilon();
    protected final IPrimitiveValueProvider[] keyValueProviders;

    protected IRTreePolicy rtreePolicy;

    public RTreeNSMFrame(ITreeIndexTupleWriter tupleWriter, IPrimitiveValueProvider[] keyValueProviders,
            RTreePolicyType rtreePolicyType) {
        super(tupleWriter, new UnorderedSlotManager());
        this.tuples = new ITreeIndexTupleReference[keyValueProviders.length];
        for (int i = 0; i < keyValueProviders.length; i++) {
            this.tuples[i] = tupleWriter.createTupleReference();
        }
        cmpFrameTuple = tupleWriter.createTupleReference();
        this.keyValueProviders = keyValueProviders;

        if (rtreePolicyType == RTreePolicyType.RTREE) {
            rtreePolicy = new RTreePolicy(tupleWriter, keyValueProviders, cmpFrameTuple, totalFreeSpaceOff);
        } else {
            rtreePolicy = new RStarTreePolicy(tupleWriter, keyValueProviders, cmpFrameTuple, totalFreeSpaceOff);
        }
    }

    private static double computeDoubleEpsilon() {
        double doubleEpsilon = 1.0;

        do {
            doubleEpsilon /= 2.0;
        } while (1.0 + (doubleEpsilon / 2.0) != 1.0);
        return doubleEpsilon;
    }

    public static double doubleEpsilon() {
        return doubleEpsilon;
    }

    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        buf.putLong(pageNsnOff, 0);
        buf.putInt(rightPageOff, -1);
    }

    public void setTupleCount(int tupleCount) {
        buf.putInt(tupleCountOff, tupleCount);
    }

    @Override
    public void setPageNsn(long pageNsn) {
        buf.putLong(pageNsnOff, pageNsn);
    }

    @Override
    public long getPageNsn() {
        return buf.getLong(pageNsnOff);
    }

    @Override
    protected void resetSpaceParams() {
        buf.putInt(freeSpaceOff, rightPageOff + 4);
        buf.putInt(totalFreeSpaceOff, buf.capacity() - (rightPageOff + 4));
    }

    @Override
    public int getRightPage() {
        return buf.getInt(rightPageOff);
    }

    @Override
    public void setRightPage(int rightPage) {
        buf.putInt(rightPageOff, rightPage);
    }

    public ITreeIndexTupleReference[] getTuples() {
        return tuples;
    }

    @Override
    public void split(ITreeIndexFrame rightFrame, ITupleReference tuple, ISplitKey splitKey) {
        rtreePolicy.split(this, buf, rightFrame, slotManager, frameTuple, tuple, splitKey);
    }

    abstract public int getTupleSize(ITupleReference tuple);

    public void adjustMBRImpl(ITreeIndexTupleReference[] tuples) {
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
        for (int i = 0; i < tuples.length; i++) {
            tuples[i].setFieldCount(getFieldCount());
            tuples[i].resetByTupleIndex(this, 0);
        }

        adjustMBRImpl(tuples);
    }

    public abstract int getFieldCount();

    @Override
    public int getPageHeaderSize() {
        return rightPageOff + 4;
    }

    @Override
    public void setMultiComparator(MultiComparator cmp) {
        // currently, R-Tree Frames are unsorted
    }
}