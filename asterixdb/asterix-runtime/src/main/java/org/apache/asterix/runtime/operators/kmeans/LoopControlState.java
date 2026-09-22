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
package org.apache.asterix.runtime.operators.kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.Semaphore;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.state.IStateObject;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;

/**
 * The per-partition loop-back rendezvous of one k-means loop sub-graph, shared via joblet-scoped state by
 * the co-located Cost, Sample and Release tasks on one NC: Cost creates it under a per-partition token,
 * the others retrieve it by that token.
 * <p>
 * It carries the {@link Semaphore} permit that paces the loop: the head awaits a turn after emitting a
 * round, the tail hands it back after appending the round's draws; that release/acquire pair is the
 * happens-before that makes the appended run-file size visible to the next round. A failing tail calls
 * {@link #abort()}, which raises the head. Readers must resolve this state on first frame, not in
 * {@code open()}; see {@link #required(IHyracksTaskContext, Object)}.
 */
public final class LoopControlState extends AbstractStateObject {

    // Not serialized: this state never leaves the NC (joblet-local). The semaphore is created empty; the loop tail
    // grants one permit per completed round/iteration.
    private final transient Semaphore permit = new Semaphore(0);

    // Lloyd loop only: the centroid set each iteration replaces. Permit-ordered; see CentroidStore.
    private final transient CentroidStore centroids;

    // Set by abort() from a sibling task's fail(); checked after every acquire so a woken waiter raises.
    private volatile transient boolean aborted;

    public LoopControlState(JobId jobId, Object id, TaskId taskId) {
        super(jobId, id);
        this.centroids = new CentroidStore.Spilling(jobId, taskId);
    }

    /** The loop tail hands the turn back to the head, having published everything the next round reads. */
    public void releaseTurn() {
        permit.release();
    }

    /**
     * The loop head waits for its next turn, unbounded, and raises when the loop was aborted. A deadline
     * would fail a healthy slow query; liveness comes from job aborts (a thread interrupt, since
     * {@link Semaphore#acquire()} is interruptible) and from {@link #abortAll} on a failing sibling.
     *
     * @param what names the waiting loop, for the error message.
     */
    public void awaitTurn(String what) throws HyracksDataException, InterruptedException {
        permit.acquire();
        if (aborted) {
            throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE, what
                    + ": aborted because a co-located task in the loop failed; see that task's error for the cause");
        }
    }

    /**
     * Wakes every waiter, permanently: this partition's loop cannot make progress. Called from the
     * {@code fail()} of the tasks that would have released the turn. It covers the window between a sibling
     * failing and the job-level abort arriving, failures that never route through {@code Task.abort()}, and
     * it makes the head raise on a loop whose tail is gone. The cause travels through the job, not here.
     */
    public void abort() {
        aborted = true;
        permit.release(Short.MAX_VALUE);
    }

    /** The centroid handoff for a Lloyd loop partition; unused by the oversampling loop. */
    public CentroidStore getCentroids() {
        return centroids;
    }

    /** The joblet-state id under which Op1 registers, and Op3/Op5 look up, this partition's control state. */
    public static Object controlStateId(String loopKey, int partition) {
        return loopKey + "#loopctl#" + partition;
    }

    /** The joblet-state id of this partition's shared pool run file ({@code MaterializerTaskState}). */
    public static Object poolStateId(String loopKey, int partition) {
        return loopKey + "#pool#" + partition;
    }

    /** The joblet-state id of this partition's shared resident-vector run file ({@code MaterializerTaskState}). */
    public static Object vectorsStateId(String loopKey, int partition) {
        return loopKey + "#vec#" + partition;
    }

    /**
     * The joblet-state id of this partition's per-round score column, written by Op1 and read by Op3. Replaced
     * each round: the loop is strictly ordered, so Op3 has finished round r-1 before Op1 starts round r.
     */
    public static Object scoreStateId(String loopKey, int partition) {
        return loopKey + "#score#" + partition;
    }

    @Override
    public void toBytes(DataOutput out) throws IOException {
        // Never serialized; joblet-local.
    }

    @Override
    public void fromBytes(DataInput in) throws IOException {
        // Never serialized; joblet-local.
    }

    /**
     * Creates a run file addressed by one of the shared ids above: Sample and Release are separate operator
     * descriptors and cannot derive a Cost task's id, so the tasks agree on a {@code loopKey} and the
     * constructor's task id is replaced with it immediately.
     */
    public static MaterializerTaskState sharedRunFile(IHyracksTaskContext ctx, Object id) throws HyracksDataException {
        // The constructor's task id is only stored as the state's id; setId below replaces it with the
        // shared key the sibling tasks look the state up by.
        MaterializerTaskState state =
                new MaterializerTaskState(ctx.getJobletContext().getJobId(), ctx.getTaskAttemptId().getTaskId());
        state.setId(id);
        state.open(ctx);
        return state;
    }

    /**
     * Looks up a joblet state object a sibling task registered; no wait, since every caller is ordered
     * behind the registering task and a miss is a broken invariant. Within one operator, blocking edges
     * order the loop behind its store activities; across operators, a frame reaches a consumer only after
     * the loop ran. Consumers must resolve state on first frame, not in {@code open()}: Hyracks opens the
     * whole pipeline before any data flows.
     */
    public static IStateObject required(IHyracksTaskContext ctx, Object id) throws HyracksDataException {
        IStateObject state = ctx.getStateObject(id);
        if (state == null) {
            throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE,
                    "kmeans loop state '" + id + "' was not registered before its consumer ran");
        }
        return state;
    }
}
