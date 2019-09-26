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
package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.concurrent.ThreadFactory;

import org.apache.hyracks.storage.am.lsm.common.api.IIoOperationFailedCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerFactory;

/**
 * The asynchronous scheduler schedules merge operations as they arrive and allocate disk bandwidth to them
 * fairly. It avoids starvation of any merge. It is important to use this scheduler when measuring system performance.
 *
 */
public class AsynchronousScheduler extends AbstractAsynchronousScheduler {

    public static final ILSMIOOperationSchedulerFactory FACTORY = new ILSMIOOperationSchedulerFactory() {
        @Override
        public ILSMIOOperationScheduler createIoScheduler(ThreadFactory threadFactory,
                IIoOperationFailedCallback callback) {
            return new AsynchronousScheduler(threadFactory, callback);
        }

        public String getName() {
            return "async";
        }
    };

    public AsynchronousScheduler(ThreadFactory threadFactory, IIoOperationFailedCallback callback) {
        super(threadFactory, callback);
    }

    @Override
    protected void scheduleMerge(ILSMIOOperation operation) {
        executor.submit(operation);
    }

    @Override
    public void completeOperation(ILSMIOOperation operation) {
        // no op
    }
}
