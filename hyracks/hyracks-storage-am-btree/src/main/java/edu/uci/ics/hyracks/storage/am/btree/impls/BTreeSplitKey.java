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

package edu.uci.ics.hyracks.storage.am.btree.impls;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;

public class BTreeSplitKey implements ISplitKey {
    public final ITreeIndexTupleReference tuple;

    public byte[] data = null;
    public ByteBuffer buf = null;
    public int keySize = 0;

    public BTreeSplitKey(ITreeIndexTupleReference tuple) {
        this.tuple = tuple;
    }

    public void initData(int keySize) {
        // try to reuse existing memory from a lower-level split if possible
        this.keySize = keySize;
        if (data != null) {
            if (data.length < keySize + 8) {
                data = new byte[keySize + 8]; // add 8 for left and right page
                buf = ByteBuffer.wrap(data);
            }
        } else {
            data = new byte[keySize + 8]; // add 8 for left and right page
            buf = ByteBuffer.wrap(data);
        }

        tuple.resetByTupleOffset(buf, 0);
    }

    public void reset() {
        data = null;
        buf = null;
    }

    public ByteBuffer getBuffer() {
        return buf;
    }

    public ITreeIndexTupleReference getTuple() {
        return tuple;
    }

    public int getLeftPage() {
        return buf.getInt(keySize);
    }

    public int getRightPage() {
        return buf.getInt(keySize + 4);
    }

    public void setLeftPage(int leftPage) {
        buf.putInt(keySize, leftPage);
    }

    public void setRightPage(int rightPage) {
        buf.putInt(keySize + 4, rightPage);
    }

    public void setPages(int leftPage, int rightPage) {
        buf.putInt(keySize, leftPage);
        buf.putInt(keySize + 4, rightPage);
    }

    public BTreeSplitKey duplicate(ITreeIndexTupleReference copyTuple) {
        BTreeSplitKey copy = new BTreeSplitKey(copyTuple);
        copy.data = data.clone();
        copy.buf = ByteBuffer.wrap(copy.data);
        copy.tuple.setFieldCount(tuple.getFieldCount());
        copy.tuple.resetByTupleOffset(copy.buf, 0);
        return copy;
    }
}