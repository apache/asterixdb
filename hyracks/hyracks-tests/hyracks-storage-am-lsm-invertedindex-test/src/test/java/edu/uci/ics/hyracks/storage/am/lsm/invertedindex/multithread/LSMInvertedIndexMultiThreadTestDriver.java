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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.multithread;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.IIndexTestWorkerFactory;
import edu.uci.ics.hyracks.storage.am.common.IndexMultiThreadTestDriver;
import edu.uci.ics.hyracks.storage.am.common.TestOperationSelector.TestOperation;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.datagen.DataGenThread;
import edu.uci.ics.hyracks.storage.am.common.datagen.IFieldValueGenerator;

@SuppressWarnings("rawtypes")
public class LSMInvertedIndexMultiThreadTestDriver extends IndexMultiThreadTestDriver {

    protected final IFieldValueGenerator[] fieldGens;

    public LSMInvertedIndexMultiThreadTestDriver(IIndex index, IIndexTestWorkerFactory workerFactory,
            ISerializerDeserializer[] fieldSerdes, IFieldValueGenerator[] fieldGens, TestOperation[] ops,
            double[] opProbs) {
        super(index, workerFactory, fieldSerdes, ops, opProbs);
        this.fieldGens = fieldGens;
    }

    public DataGenThread createDatagenThread(int numThreads, int numBatches, int batchSize) {
        return new DataGenThread(numThreads, numBatches, batchSize, fieldSerdes, fieldGens, RANDOM_SEED, 2 * numThreads);
    }
}
