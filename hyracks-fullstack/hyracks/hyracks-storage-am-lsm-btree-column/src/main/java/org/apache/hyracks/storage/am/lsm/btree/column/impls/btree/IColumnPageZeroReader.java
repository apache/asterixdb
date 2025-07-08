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
package org.apache.hyracks.storage.am.lsm.btree.column.impls.btree;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;

public interface IColumnPageZeroReader {

    default void reset(ByteBuffer pageZeroBuf) {
        reset(pageZeroBuf, AbstractColumnBTreeLeafFrame.HEADER_SIZE);
    }

    void reset(ByteBuffer pageZeroBuf, int headerSize);

    int getColumnOffset(int columnIndex) throws HyracksDataException;

    long getColumnFilterMin(int columnIndex) throws HyracksDataException;

    long getColumnFilterMax(int columnIndex) throws HyracksDataException;

    void skipFilters();

    void skipColumnOffsets();

    int getTupleCount();

    int getLeftMostKeyOffset();

    int getRightMostKeyOffset();

    int getNumberOfPresentColumns();

    int getRelativeColumnIndex(int columnIndex) throws HyracksDataException;

    int getNextLeaf();

    int getMegaLeafNodeLengthInBytes();

    int getPageZeroCapacity();

    boolean isValidColumn(int columnIndex) throws HyracksDataException;

    void getAllColumns(BitSet presentColumns);

    ByteBuffer getPageZeroBuf();

    int populateOffsetColumnIndexPairs(long[] offsetColumnIndexPairs);

    int getNumberOfPageZeroSegments();

    BitSet getPageZeroSegmentsPages();

    int getHeaderSize();

    void resetStream(IColumnBufferProvider pageZeroSegmentBufferProvider) throws HyracksDataException;

    BitSet markRequiredPageSegments(BitSet projectedColumns, int pageZeroId, boolean markAll);

    void printPageZeroReaderInfo();

    void unPinNotRequiredPageZeroSegments() throws HyracksDataException;
}
