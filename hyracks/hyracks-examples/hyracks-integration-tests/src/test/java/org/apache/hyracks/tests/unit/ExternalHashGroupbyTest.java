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

package org.apache.hyracks.tests.unit;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryHashFunctionFamily;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.group.HashSpillableTableFactory;
import org.apache.hyracks.dataflow.std.group.ISpillableTableFactory;
import org.apache.hyracks.dataflow.std.group.external.ExternalGroupBuildOperatorNodePushable;
import org.apache.hyracks.dataflow.std.group.external.ExternalGroupWriteOperatorNodePushable;

public class ExternalHashGroupbyTest extends AbstractExternalGroupbyTest {
    ExternalGroupBuildOperatorNodePushable buildOperator;
    ExternalGroupWriteOperatorNodePushable mergeOperator;

    @Override
    protected void initial(IHyracksTaskContext ctx, int tableSize, int numFrames) {
        ISpillableTableFactory tableFactory = new HashSpillableTableFactory(
                new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE });
        buildOperator = new ExternalGroupBuildOperatorNodePushable(ctx, this.hashCode(), tableSize,
                numFrames * ctx.getInitialFrameSize(), keyFields, numFrames, comparatorFactories,
                normalizedKeyComputerFactory, partialAggrInPlace, inRecordDesc, outputRec, tableFactory);
        mergeOperator = new ExternalGroupWriteOperatorNodePushable(ctx, this.hashCode(), tableFactory, outputRec,
                outputRec, numFrames, keyFieldsAfterPartial, normalizedKeyComputerFactory, comparatorFactories,
                finalAggrInPlace);
    }

    @Override
    protected IFrameWriter getBuilder() {
        return buildOperator;
    }

    @Override
    protected AbstractUnaryOutputSourceOperatorNodePushable getMerger() {
        return mergeOperator;
    }

}
