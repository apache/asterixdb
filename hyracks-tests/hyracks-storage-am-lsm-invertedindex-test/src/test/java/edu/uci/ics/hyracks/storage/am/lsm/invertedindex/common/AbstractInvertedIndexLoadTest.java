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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.common;

import java.io.IOException;

import org.junit.Test;

import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.datagen.TupleGenerator;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTestContext;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTestContext.InvertedIndexType;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTestUtils;

public abstract class AbstractInvertedIndexLoadTest extends AbstractInvertedIndexTest {

    protected final boolean bulkLoad;
    
    public AbstractInvertedIndexLoadTest(InvertedIndexType invIndexType, boolean bulkLoad) {
        super(invIndexType);
        this.bulkLoad = bulkLoad;
    }

    protected void runTest(InvertedIndexTestContext testCtx, TupleGenerator tupleGen) throws IOException,
            IndexException {
        IIndex invIndex = testCtx.getIndex();
        invIndex.create();
        invIndex.activate();

        if (bulkLoad) {
            InvertedIndexTestUtils.bulkLoadInvIndex(testCtx, tupleGen, NUM_DOCS_TO_INSERT);
        } else {
            InvertedIndexTestUtils.insertIntoInvIndex(testCtx, tupleGen, NUM_DOCS_TO_INSERT);
        }

        // Validate index and compare against expected index.
        invIndex.validate();
        if (invIndexType == InvertedIndexType.INMEMORY || invIndexType == InvertedIndexType.ONDISK) {
            InvertedIndexTestUtils.compareActualAndExpectedIndexes(testCtx);
        }

        invIndex.deactivate();
        invIndex.destroy();
    }

    @Test
    public void wordTokensInvIndexTest() throws IOException, IndexException {
        InvertedIndexTestContext testCtx = InvertedIndexTestUtils.createWordInvIndexTestContext(harness,
                getBufferCache(), invIndexType);
        TupleGenerator tupleGen = InvertedIndexTestUtils.createStringDocumentTupleGen(harness.getRandom());
        runTest(testCtx, tupleGen);
    }
}
