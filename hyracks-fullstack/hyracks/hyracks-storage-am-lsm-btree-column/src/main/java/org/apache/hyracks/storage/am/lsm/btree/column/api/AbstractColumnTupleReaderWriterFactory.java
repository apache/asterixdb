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

import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

/**
 * For columns, there are two types for {@link ITreeIndexTupleWriter} one used during write and another during read
 */
public abstract class AbstractColumnTupleReaderWriterFactory implements ITreeIndexTupleWriterFactory {
    private static final long serialVersionUID = -2377235465942457248L;
    protected final int pageSize;
    protected final int maxNumberOfTuples;
    protected final double tolerance;
    protected final int maxLeafNodeSize;

    /**
     * Tuple reader/writer factory
     *
     * @param pageSize          {@link IBufferCache} page size
     * @param maxNumberOfTuples maximum number of tuples stored per a mega leaf page
     * @param tolerance         percentage of tolerated empty space
     * @param maxLeafNodeSize   the maximum size a mega leaf node can occupy
     */
    protected AbstractColumnTupleReaderWriterFactory(int pageSize, int maxNumberOfTuples, double tolerance,
            int maxLeafNodeSize) {
        this.pageSize = pageSize;
        this.maxNumberOfTuples = maxNumberOfTuples;
        this.tolerance = tolerance;
        this.maxLeafNodeSize = maxLeafNodeSize;
    }

    /**
     * Create columnar tuple writer
     *
     * @param columnMetadata writer column metadata
     */
    public abstract AbstractColumnTupleWriter createColumnWriter(IColumnMetadata columnMetadata);

    /**
     * Create columnar tuple reader
     *
     * @param columnProjectionInfo column projection info for either query or merge
     */
    public abstract AbstractColumnTupleReader createColumnReader(IColumnProjectionInfo columnProjectionInfo);

    @Override
    public final ITreeIndexTupleWriter createTupleWriter() {
        throw new UnsupportedOperationException("Operation is not supported for " + getClass().getName());
    }
}
