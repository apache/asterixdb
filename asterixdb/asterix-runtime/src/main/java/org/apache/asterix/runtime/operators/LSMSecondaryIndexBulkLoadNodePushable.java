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
package org.apache.asterix.runtime.operators;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.asterix.runtime.operators.LSMSecondaryIndexCreationTupleProcessorNodePushable.DeletedTupleCounter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PermutingTupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexDiskComponentBulkLoader;

/**
 * Note: only used with correlated merge policy
 * This operator node is used to bulk load incoming tuples (scanned from the primary index)
 * into multiple disk components of the secondary index.
 * Incoming tuple format:
 * [component pos, anti-matter flag, secondary keys, primary keys, filter values]
 */
public class LSMSecondaryIndexBulkLoadNodePushable extends AbstractLSMSecondaryIndexCreationNodePushable {
    // with tag fields
    private final PermutingFrameTupleReference tuple;
    private final PermutingTupleReference sourceTuple;
    private final PermutingTupleReference deletedKeyTuple;

    private final IIndexDataflowHelper primaryIndexHelper;
    private final IIndexDataflowHelper secondaryIndexHelper;

    private ILSMIndex primaryIndex;
    private ILSMIndex secondaryIndex;

    private LSMIndexDiskComponentBulkLoader componentBulkLoader;
    private int currentComponentPos = -1;

    public LSMSecondaryIndexBulkLoadNodePushable(IHyracksTaskContext ctx, int partition, RecordDescriptor inputRecDesc,
            IIndexDataflowHelperFactory primaryIndexHelperFactory,
            IIndexDataflowHelperFactory secondaryIndexHelperFactory, int[] fieldPermutation, int numTagFields,
            int numSecondaryKeys, int numPrimaryKeys, boolean hasBuddyBTree) throws HyracksDataException {
        super(ctx, partition, inputRecDesc, numTagFields, numSecondaryKeys, numPrimaryKeys, hasBuddyBTree);

        this.primaryIndexHelper =
                primaryIndexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
        this.secondaryIndexHelper =
                secondaryIndexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
        this.tuple = new PermutingFrameTupleReference(fieldPermutation);

        int[] sourcePermutation = new int[fieldPermutation.length - numTagFields];
        for (int i = 0; i < sourcePermutation.length; i++) {
            sourcePermutation[i] = i + numTagFields;
        }
        sourceTuple = new PermutingTupleReference(sourcePermutation);

        int[] deletedKeyPermutation = new int[inputRecDesc.getFieldCount() - numTagFields - numSecondaryKeys];
        for (int i = 0; i < deletedKeyPermutation.length; i++) {
            deletedKeyPermutation[i] = i + numTagFields + numSecondaryKeys;
        }
        deletedKeyTuple = new PermutingTupleReference(deletedKeyPermutation);
    }

    @Override
    public void open() throws HyracksDataException {
        super.open();
        primaryIndexHelper.open();
        primaryIndex = (ILSMIndex) primaryIndexHelper.getIndexInstance();
        secondaryIndexHelper.open();
        secondaryIndex = (ILSMIndex) secondaryIndexHelper.getIndexInstance();

    }

    @Override
    public void close() throws HyracksDataException {
        HyracksDataException closeException = null;
        try {
            endCurrentComponent();
        } catch (HyracksDataException e) {
            closeException = e;
        }

        try {
            if (primaryIndexHelper != null) {
                primaryIndexHelper.close();
            }
        } catch (HyracksDataException e) {
            if (closeException == null) {
                closeException = e;
            } else {
                closeException.addSuppressed(e);
            }
        }

        try {
            if (secondaryIndexHelper != null) {
                secondaryIndexHelper.close();
            }
        } catch (HyracksDataException e) {
            if (closeException == null) {
                closeException = e;
            } else {
                closeException.addSuppressed(e);
            }
        }

        try {
            // will definitely be called regardless of exceptions
            writer.close();
        } catch (HyracksDataException th) {
            if (closeException == null) {
                closeException = th;
            } else {
                closeException.addSuppressed(th);
            }
        }

        if (closeException != null) {
            throw closeException;
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        writer.flush();
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            try {
                // if both previous value and new value are null, then we skip
                tuple.reset(accessor, i);
                int componentPos = getComponentPos(tuple);
                if (componentPos != currentComponentPos) {
                    loadNewComponent(componentPos);
                    currentComponentPos = componentPos;
                }
                if (isAntiMatterTuple(tuple)) {
                    addAntiMatterTuple(tuple);
                } else {
                    addMatterTuple(tuple);
                }
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }
    }

    private void endCurrentComponent() throws HyracksDataException {
        if (componentBulkLoader != null) {
            componentBulkLoader.end();
            componentBulkLoader = null;
        }
    }

    private void loadNewComponent(int componentPos) throws HyracksDataException {
        endCurrentComponent();
        int numTuples = getNumDeletedTuples(componentPos);
        ILSMDiskComponent primaryComponent = primaryIndex.getDiskComponents().get(componentPos);
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(LSMIOOperationCallback.KEY_FLUSHED_COMPONENT_ID, primaryComponent.getId());
        componentBulkLoader = (LSMIndexDiskComponentBulkLoader) secondaryIndex.createBulkLoader(1.0f, false, numTuples,
                false, parameters);
    }

    private void addAntiMatterTuple(ITupleReference tuple) throws HyracksDataException {
        if (hasBuddyBTree) {
            deletedKeyTuple.reset(tuple);
            componentBulkLoader.delete(deletedKeyTuple);
        } else {
            sourceTuple.reset(tuple);
            componentBulkLoader.delete(sourceTuple);
        }
    }

    private void addMatterTuple(ITupleReference tuple) throws HyracksDataException {
        sourceTuple.reset(tuple);
        componentBulkLoader.add(sourceTuple);

    }

    private int getNumDeletedTuples(int componentPos) {
        DeletedTupleCounter counter = (DeletedTupleCounter) ctx.getStateObject(partition);
        return counter.get(componentPos);
    }

}
