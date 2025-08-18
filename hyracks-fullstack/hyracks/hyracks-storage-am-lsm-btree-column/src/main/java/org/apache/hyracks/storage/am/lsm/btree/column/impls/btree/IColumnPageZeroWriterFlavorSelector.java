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

import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;

/**
 * Strategy interface for selecting the optimal page zero writer implementation.
 * 
 * This interface implements the Strategy pattern to choose between different
 * page zero layouts based on space efficiency. The selector dynamically
 * switches between default and sparse writers to minimize storage overhead.
 * 
 * The selection is made by comparing the space requirements of both approaches
 * and choosing the more efficient one for each specific data pattern.
 */
public interface IColumnPageZeroWriterFlavorSelector {

    /**
     * Evaluates and switches the page zero writer based on space efficiency.
     * <p>
     * This method compares the space requirements of both writer implementations
     * and selects the one that uses less space. The decision is made dynamically
     * for each batch of data to optimize storage utilization.
     *
     * @param spaceOccupiedByDefaultWriter Space in bytes required by the default writer
     * @param spaceOccupiedBySparseWriter  Space in bytes required by the sparse writer
     */
    void switchPageZeroWriterIfNeeded(int spaceOccupiedByDefaultWriter, int spaceOccupiedBySparseWriter);

    byte getWriterFlag();

    /**
     * Creates the appropriate page zero reader for the given writer type.
     * <p>
     * This method is used during deserialization to create a reader that matches
     * the writer type used during serialization. The flag identifies which
     * layout was used.
     *
     * @param flag     The flag code identifying the writer type (0=default, 1=sparse)
     * @param capacity
     * @return the appropriate reader instance
     */
    IColumnPageZeroReader createPageZeroReader(byte flag, int capacity);

    void setPageZeroWriterFlag(byte writerFlag);

    /**
     * Returns the currently selected page zero writer instance.
     * 
     * @return the writer instance selected by the most recent call to switchPageZeroWriterIfNeeded
     */
    IColumnPageZeroWriter getPageZeroWriter(IColumnWriteMultiPageOp multiPageOpRef, int zerothSegmentMaxColumns,
            int maximumColumnPerPageSegment);
}
