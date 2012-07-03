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

package edu.uci.ics.hyracks.storage.am.rtree;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;

@SuppressWarnings("rawtypes")
public abstract class AbstractRTreeBulkLoadTest extends AbstractRTreeTestDriver {

    private final RTreeTestUtils rTreeTestUtils;
    private final int bulkLoadRounds;

    public AbstractRTreeBulkLoadTest(int bulkLoadRounds, boolean testRstarPolicy) {
        super(testRstarPolicy);
    	this.bulkLoadRounds = bulkLoadRounds;
        this.rTreeTestUtils = new RTreeTestUtils();
    }

    @Override
    protected void runTest(ISerializerDeserializer[] fieldSerdes,
            IPrimitiveValueProviderFactory[] valueProviderFactories, int numKeys, ITupleReference key,
            RTreePolicyType rtreePolicyType) throws Exception {
        AbstractRTreeTestContext ctx = createTestContext(fieldSerdes, valueProviderFactories, numKeys, rtreePolicyType);
        for (int i = 0; i < bulkLoadRounds; i++) {
            // We assume all fieldSerdes are of the same type. Check the first
            // one to determine which field types to generate.
            if (fieldSerdes[0] instanceof IntegerSerializerDeserializer) {
                rTreeTestUtils.bulkLoadIntTuples(ctx, numTuplesToInsert, getRandom());
            } else if (fieldSerdes[0] instanceof DoubleSerializerDeserializer) {
                rTreeTestUtils.bulkLoadDoubleTuples(ctx, numTuplesToInsert, getRandom());
            }

            rTreeTestUtils.checkScan(ctx);
            rTreeTestUtils.checkDiskOrderScan(ctx);
            rTreeTestUtils.checkRangeSearch(ctx, key);
        }
        ctx.getIndex().close();
    }

    @Override
    protected String getTestOpName() {
        return "BulkLoad";
    }
}
