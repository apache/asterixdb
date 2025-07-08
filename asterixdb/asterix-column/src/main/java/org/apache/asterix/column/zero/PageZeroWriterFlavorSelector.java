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
package org.apache.asterix.column.zero;

import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IColumnPageZeroWriter.DEFAULT_WRITER_FLAG;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IColumnPageZeroWriter.MULTI_PAGE_DEFAULT_WRITER_FLAG;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IColumnPageZeroWriter.MULTI_PAGE_SPARSE_WRITER_FLAG;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IColumnPageZeroWriter.SPARSE_WRITER_FLAG;

import org.apache.asterix.column.zero.readers.DefaultColumnPageZeroReader;
import org.apache.asterix.column.zero.readers.SparseColumnPageZeroReader;
import org.apache.asterix.column.zero.writers.DefaultColumnPageZeroWriter;
import org.apache.asterix.column.zero.writers.SparseColumnPageZeroWriter;
import org.apache.asterix.column.zero.writers.multipage.DefaultColumnMultiPageZeroReader;
import org.apache.asterix.column.zero.writers.multipage.DefaultColumnMultiPageZeroWriter;
import org.apache.asterix.column.zero.writers.multipage.SparseColumnMultiPageZeroReader;
import org.apache.asterix.column.zero.writers.multipage.SparseColumnMultiPageZeroWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IColumnPageZeroReader;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IColumnPageZeroWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IColumnPageZeroWriterFlavorSelector;

import it.unimi.dsi.fastutil.bytes.Byte2ObjectArrayMap;

/**
 * Selector that chooses between different page zero writer implementations based on space efficiency.
 *<p>
 * This class implements an optimization strategy for sparse columns:
 * - Default writer: Allocates space for all columns in the schema (suitable for dense data)
 * - Sparse writer: Only allocates space for present columns (suitable for sparse data)
 * - Multi-page writers: Variants of the above writers that support multi-page operations
 *</p>
 * The selector automatically chooses the most space-efficient option based on the actual
 * space requirements of each approach.
 */
public class PageZeroWriterFlavorSelector implements IColumnPageZeroWriterFlavorSelector {
    protected byte writerFlag = IColumnPageZeroWriter.ColumnPageZeroWriterType.DEFAULT.getWriterFlag();

    // Cache of writer instances to avoid repeated object creation
    private final Byte2ObjectArrayMap<IColumnPageZeroWriter> writers;
    private final Byte2ObjectArrayMap<IColumnPageZeroReader> readers;

    public PageZeroWriterFlavorSelector() {
        writers = new Byte2ObjectArrayMap<>();
        readers = new Byte2ObjectArrayMap<>();
    }

    /**
     * Selects the optimal page zero writer based on space efficiency.
     * <p>
     * This method compares the space requirements of both writer types and selects
     * the one that uses less space. This optimization is particularly beneficial
     * for sparse datasets where many columns may be null or missing.
     * </p>
     * @param spaceOccupiedByDefaultWriter Space in bytes required by the default writer
     * @param spaceOccupiedBySparseWriter Space in bytes required by the sparse writer
     */
    @Override
    public void switchPageZeroWriterIfNeeded(int spaceOccupiedByDefaultWriter, int spaceOccupiedBySparseWriter) {
        if (spaceOccupiedByDefaultWriter <= spaceOccupiedBySparseWriter) {
            // Default writer is more space-efficient (or equal), use it
            writerFlag = MULTI_PAGE_DEFAULT_WRITER_FLAG;
        } else {
            // Sparse writer is more space-efficient, use it
            writerFlag = MULTI_PAGE_SPARSE_WRITER_FLAG;
        }
    }

    @Override
    public void setPageZeroWriterFlag(byte writerFlag) {
        this.writerFlag = writerFlag;
    }

    /**
     * Returns the currently selected page zero writer instance.
     * Writers are cached to avoid repeated object creation.
     *
     * @return the selected writer instance
     * @throws IllegalStateException if an unsupported writer flag is encountered
     */
    @Override
    public IColumnPageZeroWriter getPageZeroWriter(IColumnWriteMultiPageOp multiPageOpRef, int zerothSegmentMaxColumns,
            int bufferCapacity) {
               return switch (writerFlag) {
                   case DEFAULT_WRITER_FLAG -> writers.computeIfAbsent(DEFAULT_WRITER_FLAG, k -> new DefaultColumnPageZeroWriter());
                   case SPARSE_WRITER_FLAG -> writers.computeIfAbsent(SPARSE_WRITER_FLAG, k -> new SparseColumnPageZeroWriter());
                   case MULTI_PAGE_DEFAULT_WRITER_FLAG -> writers.computeIfAbsent(MULTI_PAGE_DEFAULT_WRITER_FLAG, k -> new DefaultColumnMultiPageZeroWriter(multiPageOpRef, zerothSegmentMaxColumns, bufferCapacity));
                   case MULTI_PAGE_SPARSE_WRITER_FLAG -> writers.computeIfAbsent(MULTI_PAGE_SPARSE_WRITER_FLAG, k -> new SparseColumnMultiPageZeroWriter(multiPageOpRef, zerothSegmentMaxColumns, bufferCapacity));
                   default -> throw new IllegalStateException("Unsupported writer flag: " + writerFlag);
               };
    }

    @Override
    public byte getWriterFlag() {
        return writerFlag;
    }

    /**
     * Creates a page zero reader instance based on the provided flag.
     * This method is used during deserialization to create the appropriate reader
     * for the writer type that was used during serialization.
     *
     * @param flag The flag code identifying the writer type (DEFAULT_WRITER_FLAG=default, SPARSE_WRITER_FLAG=sparse)
     * @return the appropriate reader instance
     * @throws IllegalStateException if an unsupported reader flag is encountered
     */
    @Override
    public IColumnPageZeroReader createPageZeroReader(byte flag, int bufferCapacity) {
                return switch (flag) {
                    case DEFAULT_WRITER_FLAG -> readers.computeIfAbsent(DEFAULT_WRITER_FLAG, k -> new DefaultColumnPageZeroReader());
                    case SPARSE_WRITER_FLAG -> readers.computeIfAbsent(SPARSE_WRITER_FLAG, k -> new SparseColumnPageZeroReader());
                    case MULTI_PAGE_DEFAULT_WRITER_FLAG -> readers.computeIfAbsent(MULTI_PAGE_DEFAULT_WRITER_FLAG, k -> new DefaultColumnMultiPageZeroReader(bufferCapacity));
                    case MULTI_PAGE_SPARSE_WRITER_FLAG -> readers.computeIfAbsent(MULTI_PAGE_SPARSE_WRITER_FLAG, k -> new SparseColumnMultiPageZeroReader(bufferCapacity));
                    default -> throw new IllegalStateException("Unsupported reader flag: " + flag);
                };
    }
}
