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
package org.apache.asterix.common.dataflow;

import java.io.IOException;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.IFrameOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;

public class NoOpFrameOperationCallbackFactory implements IFrameOperationCallbackFactory {
    private static final long serialVersionUID = 1L;
    private static final NoOpFrameOperationCallback CALLBACK = new NoOpFrameOperationCallback();
    public static final NoOpFrameOperationCallbackFactory INSTANCE = new NoOpFrameOperationCallbackFactory();

    private NoOpFrameOperationCallbackFactory() {
    }

    @Override
    public IFrameOperationCallback createFrameOperationCallback(IHyracksTaskContext ctx,
            ILSMIndexAccessor indexAccessor) {
        return CALLBACK;
    }

    private static class NoOpFrameOperationCallback implements IFrameOperationCallback {
        @Override
        public void frameCompleted() throws HyracksDataException {
            // No Op
        }

        @Override
        public void close() throws IOException {
            // No Op
        }

        @Override
        public void fail(Throwable th) {
            // No Op
        }
    }
}
