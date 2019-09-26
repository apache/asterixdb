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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.impls.GreedyScheduler;
import org.apache.hyracks.storage.am.lsm.common.impls.NoOpIoOperationFailedCallback;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class GreedySchedulerTest {

    private static final String INDEX_1 = "index1";
    private static final String INDEX_2 = "index2";

    private final Object lock = new Object();

    @Test
    public void test() throws Exception {
        GreedyScheduler scheduler = new GreedyScheduler(r -> new Thread(r), NoOpIoOperationFailedCallback.INSTANCE);
        AtomicBoolean active1 = new AtomicBoolean(true);
        ILSMIOOperation op1 = mockMergeOperation(INDEX_1, 10, active1);

        scheduler.scheduleOperation(op1);
        // op1 is activated
        Assert.assertTrue(active1.get());

        AtomicBoolean active2 = new AtomicBoolean(true);
        ILSMIOOperation op2 = mockMergeOperation(INDEX_2, 5, active2);
        scheduler.scheduleOperation(op2);
        // op2 does not interactive with op1s
        Assert.assertTrue(active1.get());
        Assert.assertTrue(active2.get());

        scheduler.completeOperation(op2);
        Assert.assertTrue(active1.get());

        AtomicBoolean active3 = new AtomicBoolean(true);
        ILSMIOOperation op3 = mockMergeOperation(INDEX_1, 5, active3);
        scheduler.scheduleOperation(op3);
        Assert.assertTrue(active3.get());
        Assert.assertFalse(active1.get());

        AtomicBoolean active4 = new AtomicBoolean(true);
        ILSMIOOperation op4 = mockMergeOperation(INDEX_1, 7, active4);
        scheduler.scheduleOperation(op4);
        // op3 is still active
        Assert.assertFalse(active1.get());
        Assert.assertTrue(active3.get());
        Assert.assertFalse(active4.get());

        // suppose op1 is completed (though unlikely in practice), now op3 is still active
        scheduler.completeOperation(op1);
        Assert.assertTrue(active3.get());
        Assert.assertFalse(active4.get());

        // op3 completed, op4 is active
        scheduler.completeOperation(op3);
        Assert.assertTrue(active4.get());

        synchronized (lock) {
            lock.notifyAll();
        }
        scheduler.close();
    }

    private ILSMIOOperation mockMergeOperation(String index, long remainingPages, AtomicBoolean isActive)
            throws HyracksDataException {
        ILSMIOOperation mergeOp = Mockito.mock(ILSMIOOperation.class);
        Mockito.when(mergeOp.getIndexIdentifier()).thenReturn(index);
        Mockito.when(mergeOp.getIOOpertionType()).thenReturn(LSMIOOperationType.MERGE);
        Mockito.when(mergeOp.getRemainingPages()).thenReturn(remainingPages);

        Mockito.doAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                return isActive.get();
            }
        }).when(mergeOp).isActive();
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                isActive.set(true);
                return null;
            }
        }).when(mergeOp).resume();

        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                isActive.set(false);
                return null;
            }
        }).when(mergeOp).pause();

        Mockito.doAnswer(new Answer<LSMIOOperationStatus>() {
            @Override
            public LSMIOOperationStatus answer(InvocationOnMock invocation) throws Throwable {
                synchronized (lock) {
                    lock.wait();
                }
                return LSMIOOperationStatus.SUCCESS;
            }
        }).when(mergeOp).call();
        return mergeOp;

    }

}
