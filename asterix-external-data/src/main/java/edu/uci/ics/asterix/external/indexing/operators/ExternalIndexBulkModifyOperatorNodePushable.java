/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.external.indexing.operators;

import java.nio.ByteBuffer;

import edu.uci.ics.asterix.metadata.external.FilesIndexDescription;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITwoPCIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexBulkLoadOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ITwoPCIndex;

public class ExternalIndexBulkModifyOperatorNodePushable extends IndexBulkLoadOperatorNodePushable {

    private final int[] deletedFiles;
    private ArrayTupleBuilder buddyBTreeTupleBuilder = new ArrayTupleBuilder(
            FilesIndexDescription.FILE_BUDDY_BTREE_RECORD_DESCRIPTOR.getFieldCount());
    private AMutableInt32 fileNumber = new AMutableInt32(0);
    private ArrayTupleReference deleteTuple = new ArrayTupleReference();

    public ExternalIndexBulkModifyOperatorNodePushable(ExternalIndexBulkModifyOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition, int[] fieldPermutation, float fillFactor, long numElementsHint,
            IRecordDescriptorProvider recordDescProvider, int[] deletedFiles) {
        super(opDesc, ctx, partition, fieldPermutation, fillFactor, false, numElementsHint, false, recordDescProvider);
        this.deletedFiles = deletedFiles;
    }

    // We override this method to do two things
    // when creating the bulkLoader, it creates a transaction bulk loader
    // It uses the bulkLoader to insert delete tuples for the deleted files
    @Override
    public void open() throws HyracksDataException {
        RecordDescriptor recDesc = recDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
        accessor = new FrameTupleAccessor(recDesc);
        indexHelper.open();
        index = indexHelper.getIndexInstance();
        try {
            // Transactional BulkLoader
            bulkLoader = ((ITwoPCIndex) index).createTransactionBulkLoader(fillFactor, verifyInput, deletedFiles.length,
                    checkIfEmptyIndex);
            // Delete files
            for (int i = 0; i < deletedFiles.length; i++) {
                fileNumber.setValue(deletedFiles[i]);
                FilesIndexDescription.getBuddyBTreeTupleFromFileNumber(deleteTuple, buddyBTreeTupleBuilder, fileNumber);
                ((ITwoPCIndexBulkLoader) bulkLoader).delete(deleteTuple);
            }
        } catch (Exception e) {
            ((ITwoPCIndexBulkLoader) bulkLoader).abort();
            indexHelper.close();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            tuple.reset(accessor, i);
            try {
                bulkLoader.add(tuple);
            } catch (IndexException e) {
                ((ITwoPCIndexBulkLoader) bulkLoader).abort();
                throw new HyracksDataException(e);
            }
        }
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            bulkLoader.end();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            indexHelper.close();
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        ((ITwoPCIndexBulkLoader) bulkLoader).abort();
    }
}
