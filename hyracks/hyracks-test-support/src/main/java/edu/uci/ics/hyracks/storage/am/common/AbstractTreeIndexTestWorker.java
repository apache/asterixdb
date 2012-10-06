/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.common;

import java.util.Random;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.TestOperationSelector.TestOperation;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.datagen.DataGenThread;
import edu.uci.ics.hyracks.storage.am.common.datagen.TupleBatch;

public abstract class AbstractTreeIndexTestWorker extends Thread implements ITreeIndexTestWorker {
    private Random rnd = new Random();
    private final DataGenThread dataGen;
    private final TestOperationSelector opSelector;
    private final int numBatches;
    
    protected final IIndexAccessor indexAccessor;
    
    public AbstractTreeIndexTestWorker(DataGenThread dataGen, TestOperationSelector opSelector, ITreeIndex index, int numBatches) {
        this.dataGen = dataGen;
        this.opSelector = opSelector;
        this.numBatches = numBatches;
        indexAccessor = index.createAccessor();
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
        try {
            while (cursor.hasNext()) {
                cursor.next();
            }
        } finally {
            cursor.close();
        }
    }
}
