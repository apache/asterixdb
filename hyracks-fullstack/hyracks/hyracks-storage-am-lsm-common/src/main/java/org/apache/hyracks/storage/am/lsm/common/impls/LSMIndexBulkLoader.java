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
package org.apache.hyracks.storage.am.lsm.common.impls;

import static org.apache.hyracks.storage.am.lsm.common.impls.DiskComponentMetadata.MAX_LEAF_TUPLE_COUNT_KEY;
import static org.apache.hyracks.storage.am.lsm.common.impls.DiskComponentMetadata.THETA_INSERT_DELETE_SKETCH_KEY;

import java.io.IOException;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndexBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentMetadata;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleWriter;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.ISketchSampler;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class LSMIndexBulkLoader implements IChainedComponentBulkLoader {
    // The serialized theta sketch can exceed a single metadata page (e.g., small test page sizes), so it is written
    // via the chunking reader/writer, which stores it as-is when it fits and compresses+chunks it otherwise.
    private static final ChunkedComponentMetadataReaderWriter THETA_SKETCH_RW =
            new ChunkedComponentMetadataReaderWriter(THETA_INSERT_DELETE_SKETCH_KEY);

    protected final IComponentMetadata componentMetadata;
    protected final IIndexBulkLoader bulkLoader;
    protected final ISketchSampler sampler;

    public LSMIndexBulkLoader(IIndexBulkLoader bulkLoader, IComponentMetadata componentMetadata,
            ISketchSampler sampler) {
        this.bulkLoader = bulkLoader;
        this.componentMetadata = componentMetadata;
        this.sampler = sampler;
    }

    @Override
    public ITupleReference delete(ITupleReference tuple) throws HyracksDataException {
        ILSMTreeTupleWriter tupleWriter =
                (ILSMTreeTupleWriter) ((AbstractTreeIndexBulkLoader) bulkLoader).getLeafFrame().getTupleWriter();
        tupleWriter.setAntimatter(true);
        try {
            bulkLoader.add(tuple);
        } finally {
            tupleWriter.setAntimatter(false);
        }
        return tuple;
    }

    @Override
    public void cleanupArtifacts() throws HyracksDataException {
        //Noop
    }

    @Override
    public ITupleReference add(ITupleReference tuple) throws HyracksDataException {
        bulkLoader.add(tuple);
        return tuple;
    }

    @Override
    public void end() throws HyracksDataException {
        try {
            IValueReference reference = sampler.serialize();
            if (reference != null) {
                THETA_SKETCH_RW.writeMetadata(reference, componentMetadata);
            }
            // Store the max leaf tuple count for Olken rejection sampling correction
            if (bulkLoader instanceof org.apache.hyracks.storage.am.btree.impls.BTreeNSMBulkLoader) {
                int maxLeafTupleCount = ((org.apache.hyracks.storage.am.btree.impls.BTreeNSMBulkLoader) bulkLoader)
                        .getMaxLeafTupleCountOfLastPage();
                if (maxLeafTupleCount > 0) {
                    ArrayBackedValueStorage maxTupleCountStorage = new ArrayBackedValueStorage(Integer.BYTES);
                    maxTupleCountStorage.getDataOutput().writeInt(maxLeafTupleCount);
                    componentMetadata.put(MAX_LEAF_TUPLE_COUNT_KEY, maxTupleCountStorage);
                }
            }
            bulkLoader.end();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void abort() throws HyracksDataException {
        bulkLoader.abort();
    }

    @Override
    public void writeFailed(ICachedPage page, Throwable failure) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasFailed() {
        return bulkLoader.hasFailed();
    }

    @Override
    public Throwable getFailure() {
        return bulkLoader.getFailure();
    }

    @Override
    public void force() throws HyracksDataException {
        bulkLoader.force();
    }
}
