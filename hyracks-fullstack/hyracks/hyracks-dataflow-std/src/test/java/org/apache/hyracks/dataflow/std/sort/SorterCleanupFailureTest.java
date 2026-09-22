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
package org.apache.hyracks.dataflow.std.sort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * The merge activity closes its downstream writer while unwinding a failure. Closing can itself fail -- a writer
 * whose open() did not complete is the case this guards -- and a throwing finally block would replace the failure
 * being reported with the cleanup's own, discarding the only exception that explains anything.
 */
public class SorterCleanupFailureTest {

    private static final int FRAMES_LIMIT = 4;

    private static final HyracksDataException BODY_FAILURE =
            HyracksDataException.create(new IllegalStateException("the failure the user needs to see"));
    private static final HyracksDataException CLEANUP_FAILURE =
            HyracksDataException.create(new IllegalStateException("the failure raised while cleaning up"));

    @Test
    public void closeFailureIsSuppressedOntoTheOriginalFailure() throws Exception {
        RecordingWriter writer = new RecordingWriter(CLEANUP_FAILURE);
        try {
            runMergeActivity(new FailingSorter(BODY_FAILURE), writer);
            fail("expected the merge activity to rethrow the sorter's failure");
        } catch (HyracksDataException e) {
            assertSame("the original failure must be the one reported", BODY_FAILURE, e);
            assertEquals("the cleanup failure must be attached, not dropped", 1, e.getSuppressed().length);
            assertSame(CLEANUP_FAILURE, e.getSuppressed()[0]);
        }
        assertEquals("the writer must still be failed and closed", 1, writer.failCount);
        assertEquals(1, writer.closeCount);
    }

    @Test
    public void closeFailureAloneIsStillReported() throws Exception {
        RecordingWriter writer = new RecordingWriter(CLEANUP_FAILURE);
        try {
            runMergeActivity(new EmptySorter(), writer);
            fail("expected the merge activity to rethrow the close failure");
        } catch (HyracksDataException e) {
            assertSame("with nothing else to report, the close failure is the failure", CLEANUP_FAILURE, e);
            assertEquals(0, e.getSuppressed().length);
        }
    }

    private void runMergeActivity(ISorter sorter, IFrameWriter writer) throws HyracksDataException {
        IHyracksTaskContext ctx = Mockito.mock(IHyracksTaskContext.class);
        RecordDescriptor recordDescriptor = new RecordDescriptor(new ISerializerDeserializer[] { null });
        ExternalSortOperatorDescriptor sorterOperator =
                new ExternalSortOperatorDescriptor(new JobSpecification(), FRAMES_LIMIT, new int[] { 0 },
                        new IBinaryComparatorFactory[] { NoOpComparatorFactory.INSTANCE }, recordDescriptor);

        // no runs on disk, so the merge takes the skip-merging path and hands the sorter straight to the writer
        TaskId sortTaskId = new TaskId(
                new ActivityId(sorterOperator.getOperatorId(), AbstractSorterOperatorDescriptor.SORT_ACTIVITY_ID), 0);
        AbstractSorterOperatorDescriptor.SortTaskState state =
                new AbstractSorterOperatorDescriptor.SortTaskState(new JobId(0), sortTaskId);
        state.generatedRunFileReaders = new ArrayList<>();
        state.sorter = sorter;
        Mockito.when(ctx.getStateObject(sortTaskId)).thenReturn(state);

        IRecordDescriptorProvider recordDescProvider = new IRecordDescriptorProvider() {
            @Override
            public RecordDescriptor getInputRecordDescriptor(ActivityId aid, int inputIndex) {
                return recordDescriptor;
            }

            @Override
            public RecordDescriptor getOutputRecordDescriptor(ActivityId aid, int outputIndex) {
                return recordDescriptor;
            }
        };

        IOperatorNodePushable merge = sorterOperator
                .getMergeActivity(new ActivityId(sorterOperator.getOperatorId(),
                        AbstractSorterOperatorDescriptor.MERGE_ACTIVITY_ID))
                .createPushRuntime(ctx, recordDescProvider, 0, 1);
        merge.setOutputFrameWriter(0, writer, recordDescriptor);
        merge.initialize();
    }

    private static class RecordingWriter implements IFrameWriter {
        private final HyracksDataException closeFailure;
        private int failCount;
        private int closeCount;

        RecordingWriter(HyracksDataException closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public void open() throws HyracksDataException {
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        }

        @Override
        public void fail() throws HyracksDataException {
            failCount++;
        }

        @Override
        public void close() throws HyracksDataException {
            closeCount++;
            throw closeFailure;
        }
    }

    private static class FailingSorter extends EmptySorter {
        private final HyracksDataException failure;

        FailingSorter(HyracksDataException failure) {
            this.failure = failure;
        }

        @Override
        public boolean hasRemaining() {
            return true;
        }

        @Override
        public int flush(IFrameWriter writer) throws HyracksDataException {
            throw failure;
        }
    }

    private static class EmptySorter implements ISorter {
        @Override
        public boolean hasRemaining() {
            return false;
        }

        @Override
        public void reset() throws HyracksDataException {
        }

        @Override
        public void sort() throws HyracksDataException {
        }

        @Override
        public void close() throws HyracksDataException {
        }

        @Override
        public int flush(IFrameWriter writer) throws HyracksDataException {
            return 0;
        }
    }

    private static class NoOpComparatorFactory implements IBinaryComparatorFactory {
        private static final long serialVersionUID = 1L;
        static final NoOpComparatorFactory INSTANCE = new NoOpComparatorFactory();

        @Override
        public IBinaryComparator createBinaryComparator() {
            return (b1, s1, l1, b2, s2, l2) -> 0;
        }
    }
}
