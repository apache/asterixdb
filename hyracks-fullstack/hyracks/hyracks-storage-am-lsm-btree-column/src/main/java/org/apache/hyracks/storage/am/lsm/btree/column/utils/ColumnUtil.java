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
package org.apache.hyracks.storage.am.lsm.btree.column.utils;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnManager;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnTupleProjector;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.LSMColumnBTree;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentMetadata;
import org.apache.hyracks.storage.common.IIndexAccessParameters;

public class ColumnUtil {
    /**
     * Used to get the columns info from {@link IComponentMetadata#get(IValueReference, ArrayBackedValueStorage)}
     *
     * @see LSMColumnBTree#activate()
     * @see IColumnManager#activate(IValueReference)
     */
    private static final MutableArrayValueReference COLUMNS_METADATA_KEY =
            new MutableArrayValueReference("COLUMNS_METADATA".getBytes());

    private ColumnUtil() {
    }

    public static IValueReference getColumnMetadataCopy(IComponentMetadata src) throws HyracksDataException {
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        src.get(COLUMNS_METADATA_KEY, storage);
        return storage;
    }

    public static void putColumnsMetadataValue(IValueReference columnsMetadataValue, IComponentMetadata dest)
            throws HyracksDataException {
        dest.put(COLUMNS_METADATA_KEY, columnsMetadataValue);
    }

    public static int getColumnPageIndex(int columnOffset, int pageSize) {
        return (int) Math.floor((double) columnOffset / pageSize);
    }

    /**
     * Get the column starting offset within the first page
     *
     * @param offset   absolute column offset as reported by Page0
     * @param pageSize disk buffer cache page size
     * @return start offset
     */
    public static int getColumnStartOffset(int offset, int pageSize) {
        return offset % pageSize;
    }

    /**
     * Reads and returns the columns length in bytes
     * NOTE: calling this method will set the firstPage position and limit appropriately
     *
     * @param firstPage   first page of the column
     * @param startOffset starting offset of the column in firstPage see {{@link #getColumnStartOffset(int, int)}}
     * @param pageSize    disk buffer cache page size
     * @return column's length
     */
    public static int readColumnLength(ByteBuffer firstPage, int startOffset, int pageSize) {
        // Set the limit to read the length of the column
        firstPage.limit(startOffset + Integer.BYTES);
        // Set position at the start of column
        firstPage.position(startOffset);
        // Read the length of this column
        int length = firstPage.getInt();
        // Ensure the page limit to at most a full page
        firstPage.limit(Math.min(startOffset + length, pageSize));
        return length;
    }

    /**
     * Returns the number of pages a column occupies
     *
     * @param remainingLength columns remaining length. see {@link #readColumnLength(ByteBuffer, int, int)}
     * @param pageSize        disk buffer cache page size
     * @return number of pages the column occupies
     */
    public static int getNumberOfRemainingPages(int remainingLength, int pageSize) {
        return (int) Math.ceil((double) remainingLength / pageSize);
    }

    public static IColumnTupleProjector getTupleProjector(IIndexAccessParameters iap,
            IColumnTupleProjector defaultProjector) {
        IColumnTupleProjector projector =
                iap.getParameter(HyracksConstants.TUPLE_PROJECTOR, IColumnTupleProjector.class);
        return projector == null ? defaultProjector : projector;
    }
}
