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

package edu.uci.ics.hyracks.storage.am.btree;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeTestContext;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeTestUtils;

@SuppressWarnings("rawtypes")
public class UpdateTest extends BTreeTestDriver {
    private static final int numUpdateRounds = 3;
    
    @Override
    protected void runTest(ISerializerDeserializer[] fieldSerdes, int numKeys, BTreeLeafFrameType leafType, ITupleReference lowKey, ITupleReference highKey, ITupleReference prefixLowKey, ITupleReference prefixHighKey) throws Exception {
        // This is a noop because we can only update non-key fields.
        if (fieldSerdes.length == numKeys) {
            return;
        }
        
        BTreeTestContext testCtx = BTreeTestUtils.createBTreeTestContext(bufferCache, btreeFileId, fieldSerdes, numKeys, leafType);

        // We assume all fieldSerdes are of the same type. Check the first one to determine which field types to generate.
        if (fieldSerdes[0] instanceof IntegerSerializerDeserializer) {
            BTreeTestUtils.insertIntTuples(testCtx, numTuplesToInsert, rnd);
        } else if (fieldSerdes[0] instanceof UTF8StringSerializerDeserializer) {
            BTreeTestUtils.insertStringTuples(testCtx, numTuplesToInsert, rnd);
        }

        int numTuplesPerDeleteRound = (int)Math.ceil((float)testCtx.checkTuples.size() / (float)numUpdateRounds);
        for(int j = 0; j < numUpdateRounds; j++) {
            BTreeTestUtils.updateTuples(testCtx, numTuplesPerDeleteRound, rnd);

            BTreeTestUtils.checkPointSearches(testCtx);
            BTreeTestUtils.checkOrderedScan(testCtx);
            BTreeTestUtils.checkDiskOrderScan(testCtx);
            BTreeTestUtils.checkRangeSearch(testCtx, lowKey, highKey, true, true);
            if (prefixLowKey != null && prefixHighKey != null) {
                BTreeTestUtils.checkRangeSearch(testCtx, prefixLowKey, prefixHighKey, true, true);
            }
        }
    }

    @Override
    protected String getTestOpName() {
        return "Update";
    }
}
