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
package org.apache.hyracks.dataflow.std.join;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;

public class PlaneSweepJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final int JOIN_ACTIVITY_ID = 0;

    private static final long serialVersionUID = 7908488449729834977L;

    /** The spatial join predicate */
    private IPredicateEvaluator predEvaluator;

    /** Compares the left edge of two records r and s */
    private ITuplePairComparator rx1sx1;

    /** Compares the left edge of an R record to the right edge of an S record */
    private ITuplePairComparator rx1sx2;

    /** Compares the left edge of an S record to the right edge of an R record */
    private ITuplePairComparator sx1rx2;

    /** Maximum number of memory frames to cache in each dataset while doing the join */
    private int memCapacity;

    /** The underlying instance of the plane-sweep join algorithm */
//    private PlaneSweepJoin planeSweepJoin;

    /**
     * Constructs a new plane sweep join operator. The input is two datasets,
     * R and S, and the output is every pair of records (r, s) where the join
     * predicate is true.
     *
     * @param spec
     *            Job specification
     * @param rx1sx1
     *            Compares the left edge of two records from R and S.
     *            Used to run the plane-sweep join algorithm.
     * @param rx1sx2
     *            Compares the left edge of an R record to the right edge of an
     *            S record. This comparator is used to run the plane-sweep algorithm.
     * @param sx1rx2
     *            Compares the left edge of an S record to the right edge of an
     *            R record. This comparator is used to run the plane-sweep algorithm.
     * @param outputDescriptor
     * @param memCapacity
     *            Maximum number of data frames to keep in memory while performing the spatial join
     * @param predEvaluator
     */
    public PlaneSweepJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, ITuplePairComparator rx1sx1,
                                            ITuplePairComparator rx1sx2, ITuplePairComparator sx1rx2, RecordDescriptor outputDescriptor,
                                            int memCapacity, IPredicateEvaluator predEvaluator) {
        super(spec, 2, 1);
        this.rx1sx1 = rx1sx1;
        this.rx1sx2 = rx1sx2;
        this.sx1rx2 = sx1rx2;
        // TODO can I create the output record descriptor here by concatenating
        // the two input descriptors, or do I have to take it as input?
        this.outRecDescs[0] = outputDescriptor;
        this.memCapacity = memCapacity;
        this.predEvaluator = predEvaluator;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId joinId = new ActivityId(getOperatorId(), JOIN_ACTIVITY_ID);
        PlaneSweepJoinActivityNode joinActivity = new PlaneSweepJoinActivityNode(joinId);

        builder.addActivity(this, joinActivity);
        builder.addSourceEdge(0, joinActivity, 0);
        builder.addSourceEdge(1, joinActivity, 1);
        builder.addTargetEdge(0, joinActivity, 0);
    }

    /**
     * The core spatial join code is embedded in this activity node.
     *
     * @author Ahmed Eldawy
     */
    public class PlaneSweepJoinActivityNode extends AbstractActivityNode {

        public PlaneSweepJoinActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
            return null;
        }
    }
}
