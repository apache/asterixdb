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

package edu.uci.ics.hyracks.storage.am.lsm.common.frames;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrame;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class LSMComponentFilterFrame implements ILSMComponentFilterFrame {

    // This page consists of two tuples that represents the minimum and maximum tuples in an LSM component.

    // A-one byte to indicate whether the filter tuples were set yet.
    private static final int minTupleIsSetIndicatorOff = 0;
    private static final int maxTupleIsSetIndicatorOff = 1;

    private final int minTupleOff;
    private final int maxTupleOff;

    private final ITreeIndexTupleWriter tupleWriter;

    protected ICachedPage page = null;
    protected ByteBuffer buf = null;

    private ITreeIndexTupleReference minTuple;
    private ITreeIndexTupleReference maxTuple;

    public LSMComponentFilterFrame(ITreeIndexTupleWriter tupleWriter, int pageSize) {
        this.tupleWriter = tupleWriter;
        this.minTupleOff = maxTupleIsSetIndicatorOff + 1;
        this.maxTupleOff = maxTupleIsSetIndicatorOff + 1 + (pageSize / 2);

        this.minTuple = tupleWriter.createTupleReference();
        this.maxTuple = tupleWriter.createTupleReference();
    }

    @Override
    public void initBuffer() {
        buf.put(minTupleIsSetIndicatorOff, (byte) 0);
        buf.put(maxTupleIsSetIndicatorOff, (byte) 0);
    }

    @Override
    public ICachedPage getPage() {
        return page;
    }

    @Override
    public void setPage(ICachedPage page) {
        this.page = page;
        this.buf = page.getBuffer();
    }

    @Override
    public void writeMinTuple(ITupleReference tuple) {
        tupleWriter.writeTuple(tuple, buf.array(), minTupleOff);
        buf.put(minTupleIsSetIndicatorOff, (byte) 1);
    }

    @Override
    public void writeMaxTuple(ITupleReference tuple) {
        tupleWriter.writeTuple(tuple, buf.array(), maxTupleOff);
        buf.put(maxTupleIsSetIndicatorOff, (byte) 1);
    }

    @Override
    public boolean isMinTupleSet() {
        return buf.get(minTupleIsSetIndicatorOff) == (byte) 1 ? true : false;
    }

    @Override
    public boolean isMaxTupleSet() {
        return buf.get(maxTupleIsSetIndicatorOff) == (byte) 1 ? true : false;
    }

    @Override
    public ITupleReference getMinTuple() {
        minTuple.resetByTupleOffset(buf, minTupleOff);
        return minTuple;
    }

    @Override
    public ITupleReference getMaxTuple() {
        maxTuple.resetByTupleOffset(buf, maxTupleOff);
        return maxTuple;
    }
}
