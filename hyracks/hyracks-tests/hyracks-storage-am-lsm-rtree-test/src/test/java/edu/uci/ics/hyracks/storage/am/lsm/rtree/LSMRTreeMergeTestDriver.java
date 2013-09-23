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

package edu.uci.ics.hyracks.storage.am.lsm.rtree;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.config.AccessMethodTestsConfig;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.AbstractLSMRTree;
import edu.uci.ics.hyracks.storage.am.rtree.AbstractRTreeTestContext;
import edu.uci.ics.hyracks.storage.am.rtree.AbstractRTreeTestDriver;
import edu.uci.ics.hyracks.storage.am.rtree.RTreeTestUtils;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;

@SuppressWarnings("rawtypes")
public abstract class LSMRTreeMergeTestDriver extends AbstractRTreeTestDriver {

    private final RTreeTestUtils rTreeTestUtils;

    public LSMRTreeMergeTestDriver(boolean testRstarPolicy) {
        super(testRstarPolicy);
        this.rTreeTestUtils = new RTreeTestUtils();
    }

    @Override
    protected void runTest(ISerializerDeserializer[] fieldSerdes,
            IPrimitiveValueProviderFactory[] valueProviderFactories, int numKeys, ITupleReference key,
            RTreePolicyType rtreePolicyType) throws Exception {

        AbstractRTreeTestContext ctx = createTestContext(fieldSerdes, valueProviderFactories, numKeys, rtreePolicyType);
        ctx.getIndex().create();
        ctx.getIndex().activate();
        // Start off with one tree bulk loaded.
        // We assume all fieldSerdes are of the same type. Check the first one
        // to determine which field types to generate.
        if (fieldSerdes[0] instanceof IntegerSerializerDeserializer) {
            rTreeTestUtils.bulkLoadIntTuples(ctx, numTuplesToInsert, getRandom());
        } else if (fieldSerdes[0] instanceof DoubleSerializerDeserializer) {
            rTreeTestUtils.bulkLoadDoubleTuples(ctx, numTuplesToInsert, getRandom());
        }

        int maxTreesToMerge = AccessMethodTestsConfig.LSM_RTREE_BULKLOAD_ROUNDS;
        for (int i = 0; i < maxTreesToMerge; i++) {
            for (int j = 0; j < i; j++) {
                if (fieldSerdes[0] instanceof IntegerSerializerDeserializer) {
                    rTreeTestUtils.insertIntTuples(ctx, numTuplesToInsert, getRandom());
                    // Deactivate and the re-activate the index to force it flush its in memory component
                    ctx.getIndex().deactivate();
                    ctx.getIndex().activate();
                } else if (fieldSerdes[0] instanceof DoubleSerializerDeserializer) {
                    rTreeTestUtils.insertDoubleTuples(ctx, numTuplesToInsert, getRandom());
                    // Deactivate and the re-activate the index to force it flush its in memory component
                    ctx.getIndex().deactivate();
                    ctx.getIndex().activate();
                }
            }

            ILSMIndexAccessor accessor = (ILSMIndexAccessor) ctx.getIndexAccessor();
            accessor.scheduleMerge(NoOpIOOperationCallback.INSTANCE,
                    ((AbstractLSMRTree) ctx.getIndex()).getImmutableComponents());

            rTreeTestUtils.checkScan(ctx);
            rTreeTestUtils.checkDiskOrderScan(ctx);
            rTreeTestUtils.checkRangeSearch(ctx, key);
        }
        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    @Override
    protected String getTestOpName() {
        return "LSM Merge";
    }
}
