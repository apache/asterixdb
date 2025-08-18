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
package org.apache.asterix.column.values;

import java.util.PriorityQueue;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IColumnPageZeroWriter;

/**
 * Interface for writing column batch data to storage pages.
 * 
 * This interface abstracts the process of writing columnar data, supporting both
 * dense and sparse column layouts through the use of pluggable page zero writers.
 * The writer handles page zero metadata, primary key storage, and column data placement
 * across multiple pages with optimal space utilization.
 */
public interface IColumnBatchWriter {

    /**
     * Configures the page zero writer for this batch.
     * 
     * This method replaces the previous direct buffer approach with a more flexible
     * abstraction that supports different page zero layouts (default vs sparse).
     * The writer will be used to manage column offsets, filters, and primary key storage.
     * 
     * @param pageZeroWriter The writer implementation for page zero operations
     * @param presentColumnsIndexes Array of column indexes that contain data in this batch
     * @param numberOfColumns Total number of columns in the schema
     */
    void setPageZeroWriter(IColumnPageZeroWriter pageZeroWriter, int[] presentColumnsIndexes, int numberOfColumns)
            throws HyracksDataException;

    /**
     * Writes the primary keys' values to Page0
     *
     * @param primaryKeyWriters primary keys' writers
     */
    void writePrimaryKeyColumns(IColumnValuesWriter[] primaryKeyWriters) throws HyracksDataException;

    /**
     * Writes the non-key values to multiple pages
     *
     * @param nonKeysColumnWriters non-key values' writers
     * @return length of all columns (that includes pageZero)
     */
    int writeColumns(PriorityQueue<IColumnValuesWriter> nonKeysColumnWriters) throws HyracksDataException;

    /**
     * Close the writer
     */
    void close();
}
