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
package org.apache.asterix.common.feeds;

import org.apache.asterix.common.feeds.api.IExceptionHandler;
import org.apache.asterix.common.feeds.api.IFeedMetricCollector;
import org.apache.asterix.common.feeds.api.IFrameEventCallback;
import org.apache.asterix.common.feeds.api.IFramePostProcessor;
import org.apache.asterix.common.feeds.api.IFramePreprocessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class ComputeSideMonitoredBuffer extends MonitoredBuffer {

    public ComputeSideMonitoredBuffer(IHyracksTaskContext ctx, FeedRuntimeInputHandler inputHandler, IFrameWriter frameWriter,
            FrameTupleAccessor fta, RecordDescriptor recordDesc, IFeedMetricCollector metricCollector,
            FeedConnectionId connectionId, FeedRuntimeId runtimeId, IExceptionHandler exceptionHandler,
            IFrameEventCallback callback, int nPartitions, FeedPolicyAccessor policyAccessor) {
        super(ctx, inputHandler, frameWriter, fta, recordDesc, metricCollector, connectionId, runtimeId,
                exceptionHandler, callback, nPartitions, policyAccessor);
    }

    @Override
    protected boolean monitorProcessingRate() {
        return true;
    }

    protected boolean logInflowOutflowRate() {
        return true;
    }

    @Override
    protected boolean monitorInputQueueLength() {
        return true;
    }

    @Override
    protected IFramePreprocessor getFramePreProcessor() {
        return null;
    }

    @Override
    protected IFramePostProcessor getFramePostProcessor() {
        return null;
    }

    @Override
    protected boolean reportOutflowRate() {
        return false;
    }

    @Override
    protected boolean reportInflowRate() {
        return false;
    }

}
