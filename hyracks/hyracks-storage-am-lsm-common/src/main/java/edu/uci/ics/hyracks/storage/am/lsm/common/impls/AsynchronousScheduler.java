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

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;

public class AsynchronousScheduler implements ILSMIOOperationScheduler {
    public final static AsynchronousScheduler INSTANCE = new AsynchronousScheduler();

    private OperationPerformerThread operationPerformerThread;

    private AsynchronousScheduler() {
        operationPerformerThread = new OperationPerformerThread();
    }

    public void init(ThreadFactory threadFactory) {
        Executor executor = Executors.newCachedThreadPool(threadFactory);
        executor.execute(operationPerformerThread);
    }

    @Override
    public void scheduleOperation(ILSMIOOperation operation) throws HyracksDataException {
        operationPerformerThread.perform(operation);
    }
}

class OperationPerformerThread extends Thread {

    private final LinkedBlockingQueue<ILSMIOOperation> operationsQueue = new LinkedBlockingQueue<ILSMIOOperation>();

    public void perform(ILSMIOOperation operation) {
        operationsQueue.offer(operation);
    }

    @Override
    public void run() {
        while (true) {
            ILSMIOOperation operation;
            try {
                operation = operationsQueue.take();
            } catch (InterruptedException e) {
                break;
            }
            try {
                operation.perform();
            } catch (HyracksDataException | IndexException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
