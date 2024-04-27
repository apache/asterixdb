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
package org.apache.hyracks.storage.am.lsm.btree.column.api;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Columnar Tuple Writer:
 * <p>
 * The writer does not write directly to the page(s) buffer but write to internal temporary buffers (provided by
 * {@link IColumnWriteMultiPageOp} until the header page (or page0) is full or the number of tuples equals to the
 * {@link #getMaxNumberOfTuples()}
 * Then, the columns are flushed to disk.
 * <p>
 * Contract:
 * - Initially, the writer has to set multiPageOp by calling {@link #init(IColumnWriteMultiPageOp)}
 * - For each write, the caller should check if adding a tuple does not exceed the {@link #getMaxNumberOfTuples()} or
 * the on-disk page size (called stopping condition)
 * - If the stopping condition is reached, then {@link #flush(ByteBuffer)} needed to be called
 * <p>
 * Hyracks visibility:
 * - Columns are written as blobs (i.e., not interpretable by Hyracks)
 * - Hyracks only aware of where each column at
 */
public abstract class AbstractColumnTupleWriter extends AbstractTupleWriterDisabledMethods {
    /**
     * Set the writer with {@link IColumnWriteMultiPageOp} to allocate columns for their writers
     *
     * @param multiPageOp multiPageOp
     */
    public abstract void init(IColumnWriteMultiPageOp multiPageOp) throws HyracksDataException;

    /**
     * @return The current number of columns
     */
    public abstract int getNumberOfColumns();

    /**
     * Currently, a column offset takes 4-byte (fixed). But in the future, we can reformat the offsets. For example,
     * we can store index-offset pairs if we encounter a sparse columns (i.e., most columns are just nulls).
     *
     * @return the size needed to store columns' offsets
     */
    public final int getColumnOffsetsSize() {
        return Integer.BYTES * getNumberOfColumns();
    }

    /**
     * @return maximum number of tuples to be stored per page (i.e., page0)
     */
    public abstract int getMaxNumberOfTuples();

    /**
     * @return page0 occupied space
     */
    public abstract int getOccupiedSpace();

    /**
     * Writes the tuple into a temporary internal buffers
     *
     * @param tuple The tuple to be written
     */
    public abstract void writeTuple(ITupleReference tuple) throws HyracksDataException;

    /**
     * Flush all columns from the internal buffers to the page buffer
     *
     * @return total flushed length (including page zero)
     */
    public abstract int flush(ByteBuffer pageZero) throws HyracksDataException;

    /**
     * Close the current writer and release all allocated temporary buffers
     */
    public abstract void close();
}
