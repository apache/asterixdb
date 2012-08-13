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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk;

import java.io.IOException;
import java.util.Iterator;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.OrderedIndexTestUtils;
import edu.uci.ics.hyracks.storage.am.common.CheckTuple;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.datagen.TupleGenerator;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory.AbstractSingleComponentInvertedIndexTest;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTestContext;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTestContext.InvertedIndexType;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

@SuppressWarnings("rawtypes")
public class OnDiskInvertedIndexBulkLoadTest extends AbstractSingleComponentInvertedIndexTest {

    public OnDiskInvertedIndexBulkLoadTest() {
        super(InvertedIndexType.ONDISK);
    }

    @Override
    public void loadIndex(TupleGenerator tupleGen, InvertedIndexTestContext testCtx) throws IOException, IndexException {
        // First generate the expected index by inserting the documents one-by-one.
        for (int i = 0; i < NUM_DOCS_TO_INSERT; i++) {
            ITupleReference tuple = tupleGen.next();
            testCtx.insertCheckTuples(tuple);
        }
        ISerializerDeserializer[] fieldSerdes = testCtx.getFieldSerdes();

        // Use the expected index to bulk-load the actual index.
        IIndexBulkLoader bulkLoader = testCtx.getIndex().createBulkLoader(1.0f, false);
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(testCtx.getFieldSerdes().length);
        ArrayTupleReference tuple = new ArrayTupleReference();
        Iterator<CheckTuple> checkTupleIter = testCtx.getCheckTuples().iterator();
        while (checkTupleIter.hasNext()) {
            CheckTuple checkTuple = checkTupleIter.next();
            OrderedIndexTestUtils.createTupleFromCheckTuple(checkTuple, tupleBuilder, tuple, fieldSerdes);
            bulkLoader.add(tuple);
        }
        bulkLoader.end();
    }

    @Override
    public IBufferCache getBufferCache() {
        return harness.getDiskBufferCache();
    }
}
