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

package org.apache.hyracks.api.rewriter.runtime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * The runtime of a SuperActivity, which internally executes a DAG of one-to-one
 * connected activities in a single thread.
 */
public class SuperActivityOperatorNodePushable implements IOperatorNodePushable {
    private final Map<ActivityId, IOperatorNodePushable> operatorNodePushables = new HashMap<>();
    private final List<IOperatorNodePushable> operatorNodePushablesBFSOrder = new ArrayList<>();
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
        runInParallel(op -> op.initialize());
    }

    @Override
    public void deinitialize() throws HyracksDataException {
        runInParallel(op -> op.deinitialize());
    }

    private void init() throws HyracksDataException {
        Map<ActivityId, IOperatorNodePushable> startOperatorNodePushables = new HashMap<>();
        Queue<Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>>> childQueue = new LinkedList<>();
        List<IConnectorDescriptor> outputConnectors;

        /**
         * Set up the source operators
         */
        for (Entry<ActivityId, IActivity> entry : startActivities.entrySet()) {
            IOperatorNodePushable opPushable =
                    entry.getValue().createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
            startOperatorNodePushables.put(entry.getKey(), opPushable);
            operatorNodePushablesBFSOrder.add(opPushable);
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
                destOp = channel.getRight().getLeft().createPushRuntime(ctx, recordDescProvider, partition,
                        nPartitions);
                operatorNodePushablesBFSOrder.add(destOp);
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
        return operatorNodePushable.getInputFrameWriter(activityIdInputIndex.getRight());
    }

    @Override
    public String getDisplayName() {
        return "Super Activity " + parent.getActivityMap().values().toString();
    }

    @FunctionalInterface
    interface OperatorNodePushableAction {
        void run(IOperatorNodePushable op) throws HyracksDataException;
    }

    private void runInParallel(OperatorNodePushableAction action) throws HyracksDataException {
        List<Future<Void>> tasks = new ArrayList<>();
        final Semaphore startSemaphore = new Semaphore(1 - operatorNodePushablesBFSOrder.size());
        final Semaphore completeSemaphore = new Semaphore(1 - operatorNodePushablesBFSOrder.size());
        try {
            for (final IOperatorNodePushable op : operatorNodePushablesBFSOrder) {
                tasks.add(ctx.getExecutorService().submit(() -> {
                    startSemaphore.release();
                    try {
                        action.run(op);
                    } finally {
                        completeSemaphore.release();
                    }
                    return null;
                }));
            }
            for (Future<Void> task : tasks) {
                task.get();
            }
        } catch (Exception e) {
            try {
                startSemaphore.acquireUninterruptibly();
                for (Future<Void> task : tasks) {
                    task.cancel(true);
                }
            } finally {
                completeSemaphore.acquireUninterruptibly();
            }
            throw HyracksDataException.create(e);
        }
    }
}
