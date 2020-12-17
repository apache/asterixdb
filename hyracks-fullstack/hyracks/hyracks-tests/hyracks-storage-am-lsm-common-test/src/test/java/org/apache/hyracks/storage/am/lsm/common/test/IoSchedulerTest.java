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
package org.apache.hyracks.storage.am.lsm.common.test;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.impls.AsynchronousScheduler;
import org.apache.hyracks.storage.am.lsm.common.impls.GreedyScheduler;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpIoOperationFailedCallback;
import org.apache.hyracks.storage.am.lsm.common.test.IoSchedulerTest.MockedOperation;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class IoSchedulerTest {

    protected static final String INDEX_1 = "index1";
    protected static final String INDEX_2 = "index2";
    protected static final String INDEX_3 = "index3";
    protected static final String INDEX_4 = "index4";

    protected static class MockedOperation {
        public final ILSMIOOperation operation;
        public final AtomicBoolean scheduled = new AtomicBoolean();
        public final AtomicBoolean running = new AtomicBoolean();

        public final Semaphore completedSemaphore = new Semaphore(0);

        public MockedOperation(ILSMIOOperation mergeOp) {
            this.operation = mergeOp;
        }

        public void waitForScheduled() throws InterruptedException {
            synchronized (scheduled) {
                while (!scheduled.get()) {
                    scheduled.wait();
                }
            }
        }

        public void waitForRunning() throws InterruptedException {
            synchronized (running) {
                while (!running.get()) {
                    running.wait();
                }
            }
        }

    }

    @Test
    public void testFlush() throws Exception {
        int maxRunningFlushes = 2;

        AsynchronousScheduler scheduler = (AsynchronousScheduler) AsynchronousScheduler.FACTORY
                .createIoScheduler(r -> new Thread(r), NoOpIoOperationFailedCallback.INSTANCE, maxRunningFlushes, 0, 0);

        MockedOperation op1_1 = mockFlushOperation(INDEX_1);
        scheduler.scheduleOperation(op1_1.operation);
        op1_1.waitForScheduled();

        MockedOperation op1_2 = mockFlushOperation(INDEX_1);
        scheduler.scheduleOperation(op1_2.operation);
        Assert.assertFalse(op1_2.scheduled.get());

        MockedOperation op2_1 = mockFlushOperation(INDEX_2);
        scheduler.scheduleOperation(op2_1.operation);
        op2_1.waitForScheduled();

        MockedOperation op2_2 = mockFlushOperation(INDEX_2);
        scheduler.scheduleOperation(op2_2.operation);
        Assert.assertFalse(op2_2.scheduled.get());

        // complete op1_1
        op1_1.completedSemaphore.release();
        op1_2.waitForScheduled();

        // complete op1_2
        op1_2.completedSemaphore.release();
        Assert.assertFalse(op2_2.scheduled.get());

        // complete op2_1
        op2_1.completedSemaphore.release();
        op2_2.waitForScheduled();

        scheduler.close();
    }

    @Test
    public void testAsynchronousMerge() throws Exception {
        int maxRunningMerges = 2;

        AsynchronousScheduler scheduler =
                (AsynchronousScheduler) AsynchronousScheduler.FACTORY.createIoScheduler(r -> new Thread(r),
                        NoOpIoOperationFailedCallback.INSTANCE, 0, maxRunningMerges, maxRunningMerges);

        MockedOperation op1 = mockMergeOperation(INDEX_1, 10);
        scheduler.scheduleOperation(op1.operation);
        // op1 is scheduled
        op1.waitForScheduled();

        MockedOperation op2 = mockMergeOperation(INDEX_2, 10);
        scheduler.scheduleOperation(op2.operation);
        // op2 is scheduled
        op2.waitForScheduled();

        MockedOperation op3 = mockMergeOperation(INDEX_3, 10);
        scheduler.scheduleOperation(op3.operation);
        // op3 is waiting
        Assert.assertFalse(op3.scheduled.get());
        Assert.assertFalse(op3.running.get());

        MockedOperation op4 = mockMergeOperation(INDEX_4, 10);
        scheduler.scheduleOperation(op4.operation);
        // op4 is waiting
        Assert.assertFalse(op4.scheduled.get());
        Assert.assertFalse(op4.running.get());

        // complete op2 and wait for op3
        op2.completedSemaphore.release();
        op3.waitForScheduled();

        // complete op3 and wait for op4
        op3.completedSemaphore.release();
        op4.waitForScheduled();

        scheduler.close();
    }

    @Test
    public void testGreedyMerge() throws Exception {
        int maxScheduledMerges = 5;
        int maxRunningMerges = 2;

        GreedyScheduler scheduler = (GreedyScheduler) GreedyScheduler.FACTORY.createIoScheduler(r -> new Thread(r),
                NoOpIoOperationFailedCallback.INSTANCE, 0, maxScheduledMerges, maxRunningMerges);

        MockedOperation op1_1 = mockMergeOperation(INDEX_1, 10);
        scheduler.scheduleOperation(op1_1.operation);
        // op1_1 is running
        op1_1.waitForScheduled();
        op1_1.waitForRunning();

        MockedOperation op2 = mockMergeOperation(INDEX_2, 10);
        scheduler.scheduleOperation(op2.operation);
        // op2 is running
        op2.waitForScheduled();
        op2.waitForRunning();

        MockedOperation op3_1 = mockMergeOperation(INDEX_3, 10);
        scheduler.scheduleOperation(op3_1.operation);
        // op3_1 is scheduled, but not running
        op3_1.waitForScheduled();
        Assert.assertFalse(op3_1.running.get());

        MockedOperation op3_2 = mockMergeOperation(INDEX_3, 5);
        scheduler.scheduleOperation(op3_2.operation);
        // op3_2 is scheduled, but not running
        op3_2.waitForScheduled();
        Assert.assertFalse(op3_2.running.get());

        MockedOperation op4 = mockMergeOperation(INDEX_4, 10);
        scheduler.scheduleOperation(op4.operation);
        // op4 is scheduled, but not running
        op4.waitForScheduled();
        Assert.assertFalse(op4.running.get());

        MockedOperation op1_2 = mockMergeOperation(INDEX_1, 5);
        scheduler.scheduleOperation(op1_2.operation);
        // op1_2 is waiting, not scheduled
        Assert.assertFalse(op1_2.scheduled.get());
        Assert.assertFalse(op1_2.running.get());

        // complete op2
        op2.completedSemaphore.release();

        // op1_2 preempts op1_1 because op1_2 is smaller
        op1_2.waitForRunning();
        op1_2.waitForScheduled();

        // op3_2 is running because index3 has more merges than index4
        op3_2.waitForRunning();
        Assert.assertFalse(op3_1.running.get());

        scheduler.close();
    }

    protected MockedOperation mockMergeOperation(String index, long remainingPages) throws HyracksDataException {
        return mockOperation(index, LSMIOOperationType.MERGE, remainingPages);
    }

    protected MockedOperation mockFlushOperation(String index) throws HyracksDataException {
        return mockOperation(index, LSMIOOperationType.FLUSH, 0);
    }

    protected MockedOperation mockOperation(String index, LSMIOOperationType type, long remainingPages)
            throws HyracksDataException {
        ILSMIOOperation op = Mockito.mock(ILSMIOOperation.class);
        MockedOperation mockedOp = new MockedOperation(op);
        Mockito.when(op.getIndexIdentifier()).thenReturn(index);
        Mockito.when(op.getIOOpertionType()).thenReturn(type);
        Mockito.when(op.getRemainingPages()).thenReturn(remainingPages);

        Mockito.doAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                return mockedOp.running.get();
            }
        }).when(op).isActive();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                mockedOp.running.set(true);
                synchronized (mockedOp.running) {
                    mockedOp.running.notifyAll();
                }
                return null;
            }
        }).when(op).resume();

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                mockedOp.running.set(false);
                return null;
            }
        }).when(op).pause();

        Mockito.doAnswer(new Answer<LSMIOOperationStatus>() {
            @Override
            public LSMIOOperationStatus answer(InvocationOnMock invocation) throws Throwable {
                mockedOp.scheduled.set(true);
                synchronized (mockedOp.scheduled) {
                    mockedOp.scheduled.notifyAll();
                }
                mockedOp.completedSemaphore.acquire();
                return LSMIOOperationStatus.SUCCESS;
            }
        }).when(op).call();
        return mockedOp;

    }

}
