/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory;

import java.io.IOException;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.datagen.TupleGenerator;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTestContext;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTestContext.InvertedIndexType;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class InMemoryInvertedIndexInsertTest extends AbstractSingleComponentInvertedIndexTest {
    
    public InMemoryInvertedIndexInsertTest() {
        super(InvertedIndexType.INMEMORY);
    }

    @Override
    public void loadIndex(TupleGenerator tupleGen, InvertedIndexTestContext testCtx) throws IOException, IndexException {
        // InMemoryInvertedIndex only supports insert.
        for (int i = 0; i < NUM_DOCS_TO_INSERT; i++) {
            ITupleReference tuple = tupleGen.next();
            testCtx.getIndexAccessor().insert(tuple);
            testCtx.insertCheckTuples(tuple);
        }
    }

    @Override
    public IBufferCache getBufferCache() {
        return harness.getMemBufferCache();
    }
}
