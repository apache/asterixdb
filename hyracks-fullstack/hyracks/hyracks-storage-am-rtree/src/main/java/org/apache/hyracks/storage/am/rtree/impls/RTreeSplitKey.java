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

package org.apache.hyracks.storage.am.rtree.impls;

import java.nio.ByteBuffer;

import org.apache.hyracks.storage.am.common.api.ISplitKey;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;

public class RTreeSplitKey implements ISplitKey {
    public byte[] leftPageData = null;
    public ByteBuffer leftPageBuf = null;
    public ITreeIndexTupleReference leftTuple;

    public byte[] rightPageData = null;
    public ByteBuffer rightPageBuf = null;
    public ITreeIndexTupleReference rightTuple;

    public int keySize = 0;

    public RTreeSplitKey(ITreeIndexTupleReference leftTuple, ITreeIndexTupleReference rightTuple) {
        this.leftTuple = leftTuple;
        this.rightTuple = rightTuple;
    }

    @Override
    public void initData(int keySize) {
        // try to reuse existing memory from a lower-level split if possible
        this.keySize = keySize;
        if (leftPageData != null) {
            if (leftPageData.length < keySize + 4) {
                leftPageData = new byte[keySize + 4]; // add 4 for the page
                leftPageBuf = ByteBuffer.wrap(leftPageData);
            }
        } else {
            leftPageData = new byte[keySize + 4]; // add 4 for the page
            leftPageBuf = ByteBuffer.wrap(leftPageData);
        }
        if (rightPageData != null) {
            if (rightPageData.length < keySize + 4) {
                rightPageData = new byte[keySize + 4]; // add 4 for the page
                rightPageBuf = ByteBuffer.wrap(rightPageData);
            }
        } else {
            rightPageData = new byte[keySize + 4]; // add 4 for the page
            rightPageBuf = ByteBuffer.wrap(rightPageData);
        }

        leftTuple.resetByTupleOffset(leftPageBuf.array(), 0);
        rightTuple.resetByTupleOffset(rightPageBuf.array(), 0);
    }

    public void resetLeftPage() {
        leftPageData = null;
        leftPageBuf = null;
    }

    public void resetRightPage() {
        rightPageData = null;
        rightPageBuf = null;
    }

    public ByteBuffer getLeftPageBuffer() {
        return leftPageBuf;
    }

    public ByteBuffer getRightPageBuffer() {
        return rightPageBuf;
    }

    public ITreeIndexTupleReference getLeftTuple() {
        return leftTuple;
    }

    public ITreeIndexTupleReference getRightTuple() {
        return rightTuple;
    }

    @Override
    public int getLeftPage() {
        return leftPageBuf.getInt(keySize);
    }

    @Override
    public int getRightPage() {
        return rightPageBuf.getInt(keySize);
    }

    @Override
    public void setLeftPage(int page) {
        leftPageBuf.putInt(keySize, page);
    }

    @Override
    public void setRightPage(int page) {
        rightPageBuf.putInt(keySize, page);
    }

    public ISplitKey duplicate(ITreeIndexTupleReference copyLeftTuple, ITreeIndexTupleReference copyRightTuple) {
        RTreeSplitKey copy = new RTreeSplitKey(copyLeftTuple, copyRightTuple);
        copy.leftPageData = leftPageData.clone();
        copy.leftPageBuf = ByteBuffer.wrap(copy.leftPageData);
        copy.leftTuple.setFieldCount(leftTuple.getFieldCount());
        copy.leftTuple.resetByTupleOffset(copy.leftPageBuf.array(), 0);

        copy.rightPageData = rightPageData.clone();
        copy.rightPageBuf = ByteBuffer.wrap(copy.rightPageData);
        copy.rightTuple.setFieldCount(rightTuple.getFieldCount());
        copy.rightTuple.resetByTupleOffset(copy.rightPageBuf.array(), 0);
        return copy;
    }

    @Override
    public void reset() {
        leftPageData = null;
        leftPageBuf = null;
        rightPageData = null;
        rightPageBuf = null;
    }

    @Override
    public ByteBuffer getBuffer() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ITreeIndexTupleReference getTuple() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setPages(int leftPage, int rightPage) {
        leftPageBuf.putInt(keySize, leftPage);
        rightPageBuf.putInt(keySize, rightPage);
    }

    @Override
    public ISplitKey duplicate(ITreeIndexTupleReference copyTuple) {
        // TODO Auto-generated method stub
        return null;
    }
}
