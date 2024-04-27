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

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnTupleProjector;

public interface IColumnManager {

    /**
     * @return number of primary keys
     */
    int getNumberOfPrimaryKeys();

    /**
     * Activate the columnar manager for an empty dataset
     *
     * @return empty column metadata
     */
    IColumnMetadata activate() throws HyracksDataException;

    /**
     * Activate the column manager for a non-empty dataset
     *
     * @param metadata column metadata value from the latest component metadata
     * @return latest column metadata
     */
    IColumnMetadata activate(IValueReference metadata) throws HyracksDataException;

    /**
     * Create merge column metadata for a newly created merge component
     *
     * @param metadata         latest column metadata value stored in the metadata page
     * @param componentsTuples tuples of the merging components
     * @return column metadata for a new merged component
     */
    IColumnMetadata createMergeColumnMetadata(IValueReference metadata, List<IColumnTupleIterator> componentsTuples)
            throws HyracksDataException;

    /**
     * Create tuple projector for reading the merging components. The merge tuple projector will return all columns
     *
     * @return merge tuple projector
     */
    IColumnTupleProjector getMergeColumnProjector();
}
