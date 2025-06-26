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

import java.io.OutputStream;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Base interface for writing column values to output streams.
 * 
 * This interface provides a common abstraction for different types of column writers,
 * enabling the page zero writer implementations to handle both primary key columns
 * and regular column values uniformly. This abstraction is particularly important
 * for the sparse column architecture where different writer types need to be
 * handled consistently.
 * 
 * Implementations include:
 * - IColumnValuesWriter: For regular column data with additional metadata
 * - Primary key writers: For key columns stored directly in page zero
 */
public interface IValuesWriter {
    /**
     * Flushes the column values to the specified output stream.
     * 
     * This method writes the accumulated column data to the provided output stream.
     *
     * @param out The output stream to write the column data to
     * @throws HyracksDataException If an error occurs during the write operation
     */
    void flush(OutputStream out) throws HyracksDataException;
}
