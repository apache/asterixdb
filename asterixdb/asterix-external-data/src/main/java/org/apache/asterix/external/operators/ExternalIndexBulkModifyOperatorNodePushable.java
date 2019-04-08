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
package org.apache.asterix.external.operators;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.external.indexing.FilesIndexDescription;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.api.ITwoPCIndexBulkLoader;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexBulkLoadOperatorNodePushable;
import org.apache.hyracks.storage.am.lsm.common.api.ITwoPCIndex;

public class ExternalIndexBulkModifyOperatorNodePushable extends IndexBulkLoadOperatorNodePushable {

    private final int[] deletedFiles;
    private ArrayTupleBuilder buddyBTreeTupleBuilder =
            new ArrayTupleBuilder(FilesIndexDescription.FILE_BUDDY_BTREE_RECORD_DESCRIPTOR.getFieldCount());
    private AMutableInt32 fileNumber = new AMutableInt32(0);
    private ArrayTupleReference deleteTuple = new ArrayTupleReference();

    public ExternalIndexBulkModifyOperatorNodePushable(IIndexDataflowHelperFactory indexHelperFactory,
            IHyracksTaskContext ctx, int partition, int[] fieldPermutation, float fillFactor, boolean verifyInput,
            long numElementsHint, RecordDescriptor inputRecDesc, int[] deletedFiles,
            ITupleFilterFactory tupleFilterFactory) throws HyracksDataException {
        super(indexHelperFactory, ctx, partition, fieldPermutation, fillFactor, verifyInput, numElementsHint, false,
                inputRecDesc, tupleFilterFactory);
        this.deletedFiles = deletedFiles;
    }

    // We override this method to do two things
    // when creating the bulkLoader, it creates a transaction bulk loader
    // It uses the bulkLoader to insert delete tuples for the deleted files
    @Override
    public void open() throws HyracksDataException {
        accessor = new FrameTupleAccessor(recDesc);
        indexHelper.open();
        index = indexHelper.getIndexInstance();
        try {
            writer.open();
            // Transactional BulkLoader
            Map<String, Object> parameters = new HashMap<>();
            bulkLoader = ((ITwoPCIndex) index).createTransactionBulkLoader(fillFactor, verifyInput, deletedFiles.length,
                    parameters);
            // Delete files
            for (int i = 0; i < deletedFiles.length; i++) {
                fileNumber.setValue(deletedFiles[i]);
                FilesIndexDescription.getBuddyBTreeTupleFromFileNumber(deleteTuple, buddyBTreeTupleBuilder, fileNumber);
                ((ITwoPCIndexBulkLoader) bulkLoader).delete(deleteTuple);
            }
        } catch (Throwable e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            tuple.reset(accessor, i);
            bulkLoader.add(tuple);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        if (index != null) {
            try {
                bulkLoader.end();
            } catch (Throwable th) {
                throw HyracksDataException.create(th);
            } finally {
                try {
                    indexHelper.close();
                } finally {
                    writer.close();
                }
            }
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        if (index != null) {
            try {
                ((ITwoPCIndexBulkLoader) bulkLoader).abort();
            } finally {
                writer.fail();
            }
        }
    }
}
