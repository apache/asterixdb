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

package org.apache.hyracks.storage.am.vector.frames;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.storage.am.btree.api.ITupleAcceptor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataFrame;

/**
 * Factory for {@link VTreeDataFrame} instances. Frames are told their ordering key as
 * {@code comparatorFields} plus the comparators for those fields, so neither this class nor the frames
 * it builds interpret what the caller put there. See {@code VTreeDataTupleAccessor} for the layout.
 */
public class VTreeDataFrameFactory implements ITreeIndexFrameFactory {

    private static final long serialVersionUID = 1L;
    private final ITreeIndexTupleWriterFactory tupleWriterFactory;
    private final int vectorDimensions;

    /** Stored-tuple fields forming the ordering key, in key order, the distance first. */
    private final int[] comparatorFields;

    /**
     * Comparators for {@link #comparatorFields}, index-aligned, sliced from the same array the index
     * and its merge cursor are built with so that all three order the key identically.
     */
    private final IBinaryComparatorFactory[] keyCmpFactories;

    /** Injected from the LSM layer; decides which stored tuples a same-key write may overwrite. */
    private final ITupleAcceptor replaceAcceptor;

    public VTreeDataFrameFactory(ITreeIndexTupleWriterFactory tupleWriterFactory, int vectorDimensions,
            int[] comparatorFields, IBinaryComparatorFactory[] keyCmpFactories, ITupleAcceptor replaceAcceptor) {
        this.tupleWriterFactory = tupleWriterFactory;
        this.vectorDimensions = vectorDimensions;
        this.comparatorFields = comparatorFields;
        this.keyCmpFactories = keyCmpFactories;
        this.replaceAcceptor = replaceAcceptor;
    }

    @Override
    public IVTreeDataFrame createFrame() {
        return new VTreeDataFrame(tupleWriterFactory.createTupleWriter(), comparatorFields, keyCmpFactories,
                replaceAcceptor);
    }

    @Override
    public ITreeIndexTupleWriterFactory getTupleWriterFactory() {
        return tupleWriterFactory;
    }

    public int getVectorDimensions() {
        return vectorDimensions;
    }

    public int[] getComparatorFields() {
        return comparatorFields;
    }

    public IBinaryComparatorFactory[] getKeyCmpFactories() {
        return keyCmpFactories;
    }

    public ITupleAcceptor getReplaceAcceptor() {
        return replaceAcceptor;
    }
}
