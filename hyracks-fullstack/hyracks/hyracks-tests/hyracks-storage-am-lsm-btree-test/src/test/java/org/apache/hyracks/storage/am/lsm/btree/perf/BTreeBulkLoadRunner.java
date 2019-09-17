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

package org.apache.hyracks.storage.am.lsm.btree.perf;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.datagen.DataGenThread;
import org.apache.hyracks.storage.am.common.datagen.TupleBatch;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;

public class BTreeBulkLoadRunner extends BTreeRunner {

    protected final float fillFactor;

    public BTreeBulkLoadRunner(int numBatches, int pageSize, int numPages, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, float fillFactor) throws HyracksDataException {
        super(numBatches, pageSize, numPages, typeTraits, cmpFactories);
        this.fillFactor = fillFactor;
    }

    @Override
    public long runExperiment(DataGenThread dataGen, int numThreads) throws Exception {
        btree.create();
        long start = System.currentTimeMillis();
        IIndexBulkLoader bulkLoader = btree.createBulkLoader(1.0f, false, 0L, true, NoOpPageWriteCallback.INSTANCE);
        for (int i = 0; i < numBatches; i++) {
            TupleBatch batch = dataGen.tupleBatchQueue.take();
            for (int j = 0; j < batch.size(); j++) {
                bulkLoader.add(batch.get(j));
            }
        }
        bulkLoader.end();
        long end = System.currentTimeMillis();
        long time = end - start;
        return time;
    }
}
