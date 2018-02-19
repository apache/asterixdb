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

package org.apache.hyracks.storage.am.common;

import java.util.Random;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.TestOperationSelector.TestOperation;
import org.apache.hyracks.storage.am.common.datagen.DataGenThread;
import org.apache.hyracks.storage.am.common.datagen.TupleBatch;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;

public abstract class AbstractIndexTestWorker extends Thread implements ITreeIndexTestWorker {
    private final Random rnd;
    private final DataGenThread dataGen;
    private final TestOperationSelector opSelector;
    private final int numBatches;

    protected IIndexAccessor indexAccessor;

    public AbstractIndexTestWorker(DataGenThread dataGen, TestOperationSelector opSelector, IIndex index,
            int numBatches) throws HyracksDataException {
        this.dataGen = dataGen;
        this.opSelector = opSelector;
        this.numBatches = numBatches;
        this.rnd = new Random();
        IndexAccessParameters actx =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        this.indexAccessor = index.createAccessor(actx);
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < numBatches; i++) {
                TupleBatch batch = dataGen.getBatch();
                for (int j = 0; j < batch.size(); j++) {
                    TestOperation op = opSelector.getOp(rnd.nextInt());
                    ITupleReference tuple = batch.get(j);
                    performOp(tuple, op);
                }
                dataGen.releaseBatch(batch);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void consumeCursorTuples(IIndexCursor cursor) throws HyracksDataException {
        while (cursor.hasNext()) {
            cursor.next();
        }
    }
}
