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

package org.apache.hyracks.storage.am.rtree;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;

@SuppressWarnings("rawtypes")
public abstract class AbstractRTreeBulkLoadTest extends AbstractRTreeTestDriver {

    private final RTreeTestUtils rTreeTestUtils;
    private final boolean isPoint;

    public AbstractRTreeBulkLoadTest(boolean testRstarPolicy) {
        this(testRstarPolicy, false);
    }

    public AbstractRTreeBulkLoadTest(boolean testRstarPolicy, boolean isPoint) {
        super(testRstarPolicy);
        this.rTreeTestUtils = new RTreeTestUtils();
        this.isPoint = isPoint;
    }

    @Override
    protected void runTest(ISerializerDeserializer[] fieldSerdes,
            IPrimitiveValueProviderFactory[] valueProviderFactories, int numKeys, ITupleReference key,
            RTreePolicyType rtreePolicyType) throws Exception {
        AbstractRTreeTestContext ctx = createTestContext(fieldSerdes, valueProviderFactories, numKeys, rtreePolicyType);
        ctx.getIndex().create();
        ctx.getIndex().activate();

        // We assume all fieldSerdes are of the same type. Check the first
        // one to determine which field types to generate.
        if (fieldSerdes[0] instanceof IntegerSerializerDeserializer) {
            rTreeTestUtils.bulkLoadIntTuples(ctx, numTuplesToInsert, getRandom(), isPoint);
        } else if (fieldSerdes[0] instanceof DoubleSerializerDeserializer) {
            rTreeTestUtils.bulkLoadDoubleTuples(ctx, numTuplesToInsert, getRandom(), isPoint);
        }

        rTreeTestUtils.checkScan(ctx);
        rTreeTestUtils.checkDiskOrderScan(ctx);
        rTreeTestUtils.checkRangeSearch(ctx, key);

        ctx.getIndex().deactivate();
        ctx.getIndex().destroy();
    }

    @Override
    protected String getTestOpName() {
        return "BulkLoad";
    }
}
