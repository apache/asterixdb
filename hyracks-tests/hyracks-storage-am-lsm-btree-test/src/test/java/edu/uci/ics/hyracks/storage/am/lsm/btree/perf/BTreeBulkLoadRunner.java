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

package edu.uci.ics.hyracks.storage.am.lsm.btree.perf;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.datagen.DataGenThread;
import edu.uci.ics.hyracks.storage.am.common.datagen.TupleBatch;

public class BTreeBulkLoadRunner extends BTreeRunner {

    protected final float fillFactor;
    
    public BTreeBulkLoadRunner(int numBatches, int pageSize, int numPages, ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories, float fillFactor)
            throws HyracksDataException, BTreeException {
        super(numBatches, pageSize, numPages, typeTraits, cmpFactories);
        this.fillFactor = fillFactor;
    }

    @Override
    public long runExperiment(DataGenThread dataGen, int numThreads) throws Exception {
        btree.create(btreeFileId);
        long start = System.currentTimeMillis();
        IIndexBulkLoadContext bulkLoadCtx = btree.beginBulkLoad(1.0f);
        for (int i = 0; i < numBatches; i++) {
            TupleBatch batch = dataGen.tupleBatchQueue.take();
            for (int j = 0; j < batch.size(); j++) {
                btree.bulkLoadAddTuple(batch.get(j), bulkLoadCtx);    
            }
        }
        btree.endBulkLoad(bulkLoadCtx);
        long end = System.currentTimeMillis();
        long time = end - start;
        return time;
    }
}
