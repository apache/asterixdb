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

package edu.uci.ics.hyracks.storage.am.btree.multithread;

import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.datagen.DataGenThread;
import edu.uci.ics.hyracks.storage.am.common.test.AbstractTreeIndexTestWorker;
import edu.uci.ics.hyracks.storage.am.common.test.ITreeIndexTestWorkerFactory;
import edu.uci.ics.hyracks.storage.am.common.test.TestOperationSelector;

public class BTreeTestWorkerFactory implements ITreeIndexTestWorkerFactory {
    @Override
    public AbstractTreeIndexTestWorker create(DataGenThread dataGen, TestOperationSelector opSelector,
            ITreeIndex index, int numBatches) {
        return new BTreeTestWorker(dataGen, opSelector, index, numBatches);
    }
}
