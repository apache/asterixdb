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
 * CLUSTER BY k-means‖ initialization loop: the per-partition loop-back rendezvous shared, via
 * <b>joblet-scoped state</b>, by the co-located Cost (Op1), Sample (Op3) and Release (Op5) tasks of one
 * {@code OVERSAMPLE_LOOP} sub-graph on one NC. Because the joblet state store (see {@code Joblet.stateObjectMap})
 * spans all of a job's operators on an NC and is keyed by {@link #getId()}, Op1 creates one of these under a
 * per-partition token and Op5/Op3 retrieve it by that same token.
 * <p>
 * It carries the {@link Semaphore} permit that paces the loop: Op1 awaits a turn after emitting each round's
 * local potential, and Op5 hands the turn back after appending that round's global draws to the shared pool run
 * file. That release/acquire pair supplies the happens-before which makes the pool run file's freshly appended
 * size visible to Op1's next-round read. A tail that fails instead calls {@link #abort()}, so the head raises
 * immediately instead of waiting out the round; see {@link #abort()} for what that is and is not worth. The growing pool and the resident vectors live
 * in their own {@code MaterializerTaskState} run files (also joblet-scoped, keyed per partition); this object is
 * just the synchronization handle.
 * <p>
 * A reader (Op3/Op5) may indeed look this up before Op1 has created it, because the pipeline opens all tasks at
 * once -- but only if it looks in {@code open()}. The data-flow ordering (Op3/Op5 touch the loop only after
 * Op1's first cost) guarantees it is present by first <em>frame</em>, so readers resolve state there instead
 * and no wait is needed. See {@link #required(IHyracksTaskContext, Object)}.
 */
@org.apache.hyracks.util.annotations.AiProvenance(agent = org.apache.hyracks.util.annotations.AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = org.apache.hyracks.util.annotations.AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = org.apache.hyracks.util.annotations.AiProvenance.ContributionKind.ASSISTED)
public final class LoopControlState extends AbstractStateObject {

    // Not serialized: this state never leaves the NC (joblet-local). The semaphore is created empty; the loop tail
    // grants one permit per completed round/iteration.
    private final transient Semaphore permit = new Semaphore(0);

    // Used by the Lloyd loop only, where each iteration REPLACES the centroid set (the oversampling loop's pool
    // instead grows). Both live in run files: the set is O(k * dim) and k is the user's to choose. Reads/writes
    // are ordered by the permit; see CentroidStore.
    private final transient CentroidStore centroids;

    // Set by abort() from a sibling task's fail(). Read after every successful acquire, so a waiter that was
    // woken by the abort raises instead of proceeding on a loop that will never complete.
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
     * The loop head waits for its next turn, for as long as that takes. Raises rather than returning when
     * the loop was aborted.
     * <p>
     * Deliberately unbounded. How long a round takes is a property of the data and the parameters -- a large
     * input with a large k is entitled to hours -- so any deadline here is a guess about someone's workload,
     * and would fail a healthy query for being slow. Liveness comes from the two things that do know
     * something went wrong: a job abort, which arrives as a thread interrupt and unwinds this in
     * milliseconds because {@link Semaphore#acquire()} is interruptible, and {@link #abortAll}, which a
     * failing sibling calls. What this loop guarantees is that it does not exhaust memory, not that it
     * finishes by any particular time.
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
     * Wakes every waiter, permanently, because this partition's loop can no longer make progress. Called from
     * the {@code fail()} of the tasks that would otherwise have released the turn.
     * <p>
     * It is worth being exact about what this buys, because on the normal path a job abort already interrupts
     * the head and unwinds it in milliseconds. What this covers is narrower and still real:
     * <ul>
     * <li>the window between a sibling task failing and the job-level abort reaching this task, during which
     * the head would otherwise sit idle holding its slot, run files and heap;</li>
     * <li>a failure that never routes through {@code Task.abort()} at all;</li>
     * <li>and, independent of timing, making the head <em>raise</em> rather than proceed on a loop whose tail
     * is gone -- an interrupt alone would not distinguish that from any other cancellation.</li>
     * </ul>
     * The cause is not carried here: {@code IFrameWriter.fail()} takes no argument, and the failing task
     * reports its own exception through the job anyway.
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
     * Creates a run file addressed by one of the ids above rather than by the producing task.
     * <p>
     * {@link MaterializerTaskState}'s constructor takes a {@link TaskId}, which is how a state is normally
     * addressed -- the way {@code KMeansMerge} finds its own store activity's output. That does not work
     * across the loop: Sample and Release are separate operator descriptors and cannot derive the id of a
     * task belonging to Cost. They agree on a {@code loopKey} string instead, so the constructor's id is
     * replaced immediately. Doing that here keeps the replacement in one place instead of at each call site,
     * where it read as though the task id mattered.
     */
    public static MaterializerTaskState sharedRunFile(IHyracksTaskContext ctx, Object id) throws HyracksDataException {
        // The task id the constructor takes is only stored as the state's id, and the line below replaces it;
        // the run file is named from the joblet's workspace, not from it. So it carries no meaning here --
        // this passes the task's own id rather than fabricating one, and the id that matters is the shared
        // key set next, which is what the sibling tasks look the state up by.
        MaterializerTaskState state =
                new MaterializerTaskState(ctx.getJobletContext().getJobId(), ctx.getTaskAttemptId().getTaskId());
        state.setId(id);
        state.open(ctx);
        return state;
    }

    /**
     * Look up a joblet state object a sibling task registered. There is deliberately no wait here: every caller
     * is ordered behind the registering task, so a miss is a broken invariant rather than a race to sleep on.
     * <p>
     * Two orderings supply that guarantee.
     * <ul>
     * <li><b>Within one operator</b> — the store activities are joined to the loop activity by
     * {@code addBlockingEdge}, so the loop cannot start until they have completed and registered.</li>
     * <li><b>Across operators</b> — the loop sub-graph is a linear chain (Op1 -> ... -> Op5, all co-located by
     * an absolute partition constraint), and every downstream consumer's input descends from Op1's loop
     * output. A frame can therefore only reach a consumer after the loop activity ran, which is itself after
     * the store activities completed. Consumers must resolve state on first frame, NOT in {@code open()}:
     * Hyracks opens the whole pipeline before any data flows, so {@code open()} carries no such guarantee.</li>
     * </ul>
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
