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

package edu.uci.ics.hyracks.api.rewriter.runtime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * The runtime of a SuperActivity, which internally executes a DAG of one-to-one
 * connected activities in a single thread.
 * 
 * @author yingyib
 */
public class SuperActivityOperatorNodePushable implements IOperatorNodePushable {
    private final Map<ActivityId, IOperatorNodePushable> operatorNodePushables = new HashMap<ActivityId, IOperatorNodePushable>();
    private final List<IOperatorNodePushable> operatprNodePushablesBFSOrder = new ArrayList<IOperatorNodePushable>();
    private final Map<ActivityId, IActivity> startActivities;
    private final SuperActivity parent;
    private final IHyracksTaskContext ctx;
    private final IRecordDescriptorProvider recordDescProvider;
    private final int partition;
    private final int nPartitions;
    private int inputArity = 0;

    public SuperActivityOperatorNodePushable(SuperActivity parent, Map<ActivityId, IActivity> startActivities,
            IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        this.parent = parent;
        this.startActivities = startActivities;
        this.ctx = ctx;
        this.recordDescProvider = recordDescProvider;
        this.partition = partition;
        this.nPartitions = nPartitions;

        /**
         * initialize the writer-relationship for the internal DAG of operator
         * node pushables
         */
        try {
            init();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void initialize() throws HyracksDataException {
        /**
         * initialize operator node pushables in the BFS order
         */
        for (IOperatorNodePushable op : operatprNodePushablesBFSOrder) {
            op.initialize();
        }
    }

    public void init() throws HyracksDataException {
        Map<ActivityId, IOperatorNodePushable> startOperatorNodePushables = new HashMap<ActivityId, IOperatorNodePushable>();
        Queue<Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>>> childQueue = new LinkedList<Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>>>();
        List<IConnectorDescriptor> outputConnectors = null;

        /**
         * Set up the source operators
         */
        for (Entry<ActivityId, IActivity> entry : startActivities.entrySet()) {
            IOperatorNodePushable opPushable = entry.getValue().createPushRuntime(ctx, recordDescProvider, partition,
                    nPartitions);
            startOperatorNodePushables.put(entry.getKey(), opPushable);
            operatprNodePushablesBFSOrder.add(opPushable);
            operatorNodePushables.put(entry.getKey(), opPushable);
            inputArity += opPushable.getInputArity();
            outputConnectors = parent.getActivityOutputMap().get(entry.getKey());
            if (outputConnectors != null) {
                for (IConnectorDescriptor conn : outputConnectors) {
                    childQueue.add(parent.getConnectorActivityMap().get(conn.getConnectorId()));
                }
            }
        }

        /**
         * Using BFS (breadth-first search) to construct to runtime execution
         * DAG;
         */
        while (childQueue.size() > 0) {
            /**
             * expend the executing activities further to the downstream
             */
            if (outputConnectors != null && outputConnectors.size() > 0) {
                for (IConnectorDescriptor conn : outputConnectors) {
                    if (conn != null) {
                        childQueue.add(parent.getConnectorActivityMap().get(conn.getConnectorId()));
                    }
                }
            }

            /**
             * construct the source to destination information
             */
            Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>> channel = childQueue.poll();
            ActivityId sourceId = channel.getLeft().getLeft().getActivityId();
            int outputChannel = channel.getLeft().getRight();
            ActivityId destId = channel.getRight().getLeft().getActivityId();
            int inputChannel = channel.getRight().getRight();
            IOperatorNodePushable sourceOp = operatorNodePushables.get(sourceId);
            IOperatorNodePushable destOp = operatorNodePushables.get(destId);
            if (destOp == null) {
                destOp = channel.getRight().getLeft()
                        .createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
                operatprNodePushablesBFSOrder.add(destOp);
                operatorNodePushables.put(destId, destOp);
            }

            /**
             * construct the dataflow connection from a producer to a consumer
             */
            sourceOp.setOutputFrameWriter(outputChannel, destOp.getInputFrameWriter(inputChannel),
                    recordDescProvider.getInputRecordDescriptor(destId, inputChannel));

            /**
             * traverse to the child of the current activity
             */
            outputConnectors = parent.getActivityOutputMap().get(destId);
        }
    }

    @Override
    public void deinitialize() throws HyracksDataException {
        /**
         * de-initialize operator node pushables
         */
        for (IOperatorNodePushable op : operatprNodePushablesBFSOrder) {
            op.deinitialize();
        }
    }

    @Override
    public int getInputArity() {
        return inputArity;
    }

    @Override
    public void setOutputFrameWriter(int clusterOutputIndex, IFrameWriter writer, RecordDescriptor recordDesc)
            throws HyracksDataException {
        /**
         * set the right output frame writer
         */
        Pair<ActivityId, Integer> activityIdOutputIndex = parent.getActivityIdOutputIndex(clusterOutputIndex);
        IOperatorNodePushable opPushable = operatorNodePushables.get(activityIdOutputIndex.getLeft());
        opPushable.setOutputFrameWriter(activityIdOutputIndex.getRight(), writer, recordDesc);
    }

    @Override
    public IFrameWriter getInputFrameWriter(final int index) {
        /**
         * get the right IFrameWriter from the cluster input index
         */
        Pair<ActivityId, Integer> activityIdInputIndex = parent.getActivityIdInputIndex(index);
        IOperatorNodePushable operatorNodePushable = operatorNodePushables.get(activityIdInputIndex.getLeft());
        IFrameWriter writer = operatorNodePushable.getInputFrameWriter(activityIdInputIndex.getRight());
        return writer;
    }

    @Override
    public String getDisplayName() {
        return "Super Activity " + parent.getActivityMap().values().toString();
    }

}
