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

import java.nio.ByteBuffer;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * The current centroid set of one Lloyd loop partition, handed from the loop's tail back to its head.
 * <p>
 * Unlike the oversampling pool — which grows without bound and therefore lives in a run file — a Lloyd iteration
 * <em>replaces</em> its centroids, so the working set is bounded by the centroid count rather than by the data.
 * It is still O(k · dim) though, and k is the user's to choose, so the set lives in a run file: at 384 dimensions
 * it is a few megabytes for the k values this feature targets but hundreds for a large one, on every partition.
 * <p>
 * The interface is therefore write-once-then-replay rather than list-in, list-out. Nothing needs random access
 * to it -- the loop scores vectors against the whole set and the final emit walks it once -- and a list-shaped
 * interface would force the whole set into the heap at the boundary regardless of how it was stored.
 * <p>
 * Visibility between the writing task (Release) and the reading task (Controller) is supplied by the loop permit:
 * the writer stores before {@code release()} and the reader loads after {@code acquire()}, so the semaphore's
 * happens-before covers the handoff and no additional synchronization is required.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
public interface CentroidStore {

    /** Begins a replacement set. The current set stays readable until {@link #endPut()} swaps it. */
    void beginPut(IHyracksTaskContext ctx) throws HyracksDataException;

    /** Appends one centroid to the set being built, in centroid-index order. */
    void put(double[] centroid) throws HyracksDataException;

    /** Publishes the set that was built and discards the one it replaces. */
    void endPut() throws HyracksDataException;

    /** How many centroids the published set holds; 0 before the first {@link #endPut()}. */
    int size();

    /** Replays the published set in centroid-index order. */
    void stream(IHyracksTaskContext ctx, KMeansLoopIO.RawVectorConsumer sink) throws HyracksDataException;

    /** Releases any resources the implementation holds. Idempotent. */
    default void destroy() throws HyracksDataException {
    }

    /**
     * The default implementation: each set in a run file, one generation at a time.
     * <p>
     * Two files are live at the swap and no more -- the set being built and the one still being read -- so the
     * heap holds a frame, not a centroid set. The published set is swapped in as a whole, which is what lets the
     * reader see either the previous generation or the new one but never a partial one; the loop permit orders
     * the two tasks around that swap (writer stores before {@code release()}, reader loads after
     * {@code acquire()}), so its happens-before covers the handoff without further synchronization.
     */
    final class Spilling implements CentroidStore {
        private final JobId jobId;
        private final TaskId taskId;
        private final ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
        private MaterializerTaskState building;
        private VSizeFrame frame;
        private FrameTupleAppender appender;
        private int buildingCount;
        private volatile MaterializerTaskState published;
        private volatile int publishedCount;

        public Spilling(JobId jobId, TaskId taskId) {
            this.jobId = jobId;
            this.taskId = taskId;
        }

        @Override
        public void beginPut(IHyracksTaskContext ctx) throws HyracksDataException {
            discardBuilding();
            building = new MaterializerTaskState(jobId, taskId);
            building.open(ctx);
            frame = new VSizeFrame(ctx);
            appender = new FrameTupleAppender(frame);
            buildingCount = 0;
        }

        @Override
        public void put(double[] centroid) throws HyracksDataException {
            tb.reset();
            KMeansLoopIO.writeRawVector(tb, centroid);
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                flushFrame();
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE,
                            "a centroid is too large to fit in a frame");
                }
            }
            buildingCount++;
        }

        @Override
        public void endPut() throws HyracksDataException {
            if (appender != null && appender.getTupleCount() > 0) {
                flushFrame();
            }
            MaterializerTaskState previous = published;
            if (building != null) {
                building.close();
            }
            published = building;
            publishedCount = buildingCount;
            building = null;
            frame = null;
            appender = null;
            if (previous != null) {
                previous.deleteFile(); // the generation just replaced; no reader can still be in it
            }
        }

        @Override
        public int size() {
            return publishedCount;
        }

        @Override
        public void stream(IHyracksTaskContext ctx, KMeansLoopIO.RawVectorConsumer sink) throws HyracksDataException {
            MaterializerTaskState current = published;
            if (current != null) {
                KMeansLoopIO.streamRawVectors(current, ctx, sink);
            }
        }

        @Override
        public void destroy() throws HyracksDataException {
            discardBuilding();
            MaterializerTaskState current = published;
            published = null;
            publishedCount = 0;
            if (current != null) {
                current.close();
                current.deleteFile();
            }
        }

        private void discardBuilding() throws HyracksDataException {
            if (building != null) {
                building.close();
                building.deleteFile();
                building = null;
                frame = null;
                appender = null;
            }
        }

        private void flushFrame() throws HyracksDataException {
            ByteBuffer buffer = frame.getBuffer();
            buffer.position(0);
            buffer.limit(buffer.capacity());
            building.appendFrame(buffer);
            appender.reset(frame, true);
        }
    }
}
