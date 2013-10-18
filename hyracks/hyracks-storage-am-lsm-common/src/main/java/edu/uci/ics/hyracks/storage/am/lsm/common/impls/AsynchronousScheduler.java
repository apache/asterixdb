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
package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOpertionType;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;

public class AsynchronousScheduler implements ILSMIOOperationScheduler {
    // Since this is a asynchronous scheduler, we make sure that flush operations coming from the same lsm index
    // will be executed serially in same order of scheduling the operations. Look at asterix issue 630.

    public final static AsynchronousScheduler INSTANCE = new AsynchronousScheduler();
    private ExecutorService executor;
    private final Map<String, ILSMIOOperation> runningFlushOperations = new HashMap<String, ILSMIOOperation>();
    private final Map<String, PriorityQueue<ILSMIOOperation>> waitingFlushOperations = new HashMap<String, PriorityQueue<ILSMIOOperation>>();

    public void init(ThreadFactory threadFactory) {
        // Creating an executor with the same configuration of Executors.newCachedThreadPool. 
        executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), threadFactory) {

            @Override
            protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
                return new LSMIOOperationTask<T>(callable);
            }

            @SuppressWarnings("unchecked")
            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                super.afterExecute(r, t);
                LSMIOOperationTask<Boolean> task = (LSMIOOperationTask<Boolean>) r;
                ILSMIOOperation executedOp = task.getOperation();
                String id = executedOp.getIndexUniqueIdentifier();
                synchronized (this) {
                    runningFlushOperations.remove(id);
                    if (waitingFlushOperations.containsKey(id)) {
                        try {
                            ILSMIOOperation op = waitingFlushOperations.get(id).poll();
                            if (op != null) {
                                scheduleOperation(op);
                            } else {
                                waitingFlushOperations.remove(id);
                            }
                        } catch (HyracksDataException e) {
                            t = e.getCause();
                        }
                    }
                }
            }
        };
    }

    @Override
    public void scheduleOperation(ILSMIOOperation operation) throws HyracksDataException {
        if (operation.getIOOpertionType() == LSMIOOpertionType.MERGE) {
            executor.submit(operation);
        } else {
            String id = operation.getIndexUniqueIdentifier();
            synchronized (executor) {
                if (runningFlushOperations.containsKey(id)) {
                    if (waitingFlushOperations.containsKey(id)) {
                        waitingFlushOperations.get(id).offer(operation);
                    } else {
                        PriorityQueue<ILSMIOOperation> q = new PriorityQueue<ILSMIOOperation>();
                        q.offer(operation);
                        waitingFlushOperations.put(id, q);
                    }
                } else {
                    runningFlushOperations.put(id, operation);
                    executor.submit(operation);
                }
            }
        }
    }
}