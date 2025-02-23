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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.EnforceFrameWriter;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.ProfiledOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.api.util.InvokeUtil;
import org.apache.hyracks.util.Span;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The runtime of a SuperActivity, which internally executes a DAG of one-to-one
 * connected activities in a single thread.
 */
public class SuperActivityOperatorNodePushable implements IOperatorNodePushable {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final String CLASS_ABBREVIATION = "SAO";
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
            IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
            throws HyracksDataException {
        this.parent = parent;
        this.startActivities = startActivities;
        this.ctx = ctx;
        this.recordDescProvider = recordDescProvider;
        this.partition = partition;
        this.nPartitions = nPartitions;

        /*
         * initialize the writer-relationship for the internal DAG of operator
         * node pushables
         */
        try {
            init();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void initialize() throws HyracksDataException {
        runInParallel(IOperatorNodePushable::initialize);
    }

    @Override
    public void deinitialize() throws HyracksDataException {
        runInParallel(IOperatorNodePushable::deinitialize);
    }

    private void init() throws HyracksDataException {
        LinkedList<Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>>> childQueue = new LinkedList<>();
        List<IConnectorDescriptor> outputConnectors;
        final boolean enforce = ctx.getJobFlags().contains(JobFlag.ENFORCE_CONTRACT);
        final boolean profile = ctx.getJobFlags().contains(JobFlag.PROFILE_RUNTIME);
        /*
         * Set up the source operators
         */
        Set<Pair<Pair<IActivity, Integer>, Pair<IActivity, Integer>>> sources = new HashSet<>();
        for (Entry<ActivityId, IActivity> entry : startActivities.entrySet()) {
            IOperatorNodePushable opPushable;
            if (profile) {
                IOperatorNodePushable wrapped =
                        entry.getValue().createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
                opPushable = ProfiledOperatorNodePushable.time(wrapped, ctx, entry.getKey());
            } else {
                opPushable = entry.getValue().createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
                ProfiledOperatorNodePushable.onlyAddStats(opPushable, ctx, entry.getKey());
            }
            operatorNodePushablesBFSOrder.add(opPushable);
            operatorNodePushables.put(entry.getKey(), opPushable);
            inputArity += opPushable.getInputArity();
            outputConnectors =
                    MapUtils.getObject(parent.getActivityOutputMap(), entry.getKey(), Collections.emptyList());
            for (IConnectorDescriptor conn : outputConnectors) {
                childQueue.add(parent.getConnectorActivityMap().get(conn.getConnectorId()));
                sources.add(childQueue.peekLast());
            }
        }

        /*
         * Using BFS (breadth-first search) to construct to runtime execution DAG...
         */
        while (!childQueue.isEmpty()) {
            /*
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
                if (profile) {
                    IOperatorNodePushable wrapped = channel.getRight().getLeft().createPushRuntime(ctx,
                            recordDescProvider, partition, nPartitions);
                    destOp = ProfiledOperatorNodePushable.time(wrapped, ctx, destId);
                } else {
                    destOp = channel.getRight().getLeft().createPushRuntime(ctx, recordDescProvider, partition,
                            nPartitions);
                    ProfiledOperatorNodePushable.onlyAddStats(destOp, ctx, destId);
                }
                operatorNodePushablesBFSOrder.add(destOp);
                operatorNodePushables.put(destId, destOp);
            }

            /*
             * construct the dataflow connection from a producer to a consumer
             */
            IFrameWriter writer = destOp.getInputFrameWriter(inputChannel);
            writer = (enforce && !profile) ? EnforceFrameWriter.enforce(writer) : writer;
            sourceOp.setOutputFrameWriter(outputChannel, writer,
                    recordDescProvider.getInputRecordDescriptor(destId, inputChannel));

            /*
             * traverse to the child of the current activity
             */
            outputConnectors = MapUtils.getObject(parent.getActivityOutputMap(), destId, Collections.emptyList());

            /*
             * expend the executing activities further to the downstream
             */
            for (IConnectorDescriptor conn : outputConnectors) {
                if (conn != null) {
                    childQueue.add(parent.getConnectorActivityMap().get(conn.getConnectorId()));
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
        /*
         * set the right output frame writer
         */
        Pair<ActivityId, Integer> activityIdOutputIndex = parent.getActivityIdOutputIndex(clusterOutputIndex);
        IOperatorNodePushable opPushable = operatorNodePushables.get(activityIdOutputIndex.getLeft());
        opPushable.setOutputFrameWriter(activityIdOutputIndex.getRight(), writer, recordDesc);
    }

    @Override
    public IFrameWriter getInputFrameWriter(final int index) {
        /*
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
        List<Future<Void>> tasks = new ArrayList<>(operatorNodePushablesBFSOrder.size());
        Queue<Throwable> failures = new ArrayBlockingQueue<>(operatorNodePushablesBFSOrder.size());
        final Semaphore startSemaphore = new Semaphore(1 - operatorNodePushablesBFSOrder.size());
        final Semaphore completeSemaphore = new Semaphore(1 - operatorNodePushablesBFSOrder.size());
        Throwable root = null;
        try {
            for (final IOperatorNodePushable op : operatorNodePushablesBFSOrder) {
                tasks.add(ctx.getExecutorService().submit(() -> {
                    startSemaphore.release();
                    try {
                        Thread.currentThread().setName(CLASS_ABBREVIATION + ":" + ctx.getJobletContext().getJobId()
                                + ":" + ctx.getTaskAttemptId());
                        action.run(op);
                    } catch (Throwable th) { // NOSONAR: Must catch all causes of failure
                        failures.offer(th);
                        throw th;
                    } finally {
                        ctx.unsubscribeThreadFromStats();
                        completeSemaphore.release();
                    }
                    return null;
                }));
            }
            for (Future<Void> task : tasks) {
                task.get();
            }
        } catch (ExecutionException e) {
            root = e.getCause();
        } catch (CancellationException | RejectedExecutionException e) {
            root = e;
            // if a task has been cancelled or was rejected for execution, the executor has shut down and will no longer
            // start tasks; adjust the semaphores accordingly to allow cancelTasks() to run without getting blocked.
            completeSemaphore.release(-startSemaphore.drainPermits() + 1);
            startSemaphore.release();
        } catch (Throwable e) { // NOSONAR: Must catch all causes of failure
            root = e;
        }
        if (root != null) {
            final Throwable failure = root;
            cancelTasks(tasks, startSemaphore, completeSemaphore);
            failures.forEach(t -> ExceptionUtils.suppress(failure, t));
            throw HyracksDataException.create(failure);
        }
    }

    private void cancelTasks(List<Future<Void>> tasks, Semaphore startSemaphore, Semaphore completeSemaphore) {
        boolean cancelCompleted = false;
        try {
            startSemaphore.acquireUninterruptibly();
            cancelCompleted = cancelTasks(tasks, completeSemaphore);
        } finally {
            if (!cancelCompleted) {
                completeSemaphore.acquireUninterruptibly();
            }
        }
    }

    private static boolean cancelTasks(List<Future<Void>> tasks, Semaphore completeSemaphore) {
        Span retryWait = Span.init(5, TimeUnit.MINUTES);
        while (true) {
            for (Future<Void> task : tasks) {
                task.cancel(true);
            }
            if (acquireUninterruptibly(completeSemaphore, retryWait)) {
                return true;
            }
            LOGGER.warn("not all tasks were cancelled within 5 minutes. retrying cancelling...");
        }
    }

    private static boolean acquireUninterruptibly(Semaphore completeSemaphore, Span s) {
        s.reset();
        return InvokeUtil.getUninterruptibly(() -> {
            while (!s.elapsed()) {
                if (completeSemaphore.tryAcquire(s.remaining(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS)) {
                    return true;
                }
            }
            return false;
        });
    }
}
