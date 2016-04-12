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
package org.apache.hyracks.storage.am.lsm.btree.dataflow;

import java.util.Map;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerProvider;
import org.apache.hyracks.storage.am.lsm.common.dataflow.AbstractLSMIndexDataflowHelperFactory;

public class ExternalBTreeDataflowHelperFactory extends AbstractLSMIndexDataflowHelperFactory {

    private static final long serialVersionUID = 1L;

    private final int version;

    public ExternalBTreeDataflowHelperFactory(ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, ILSMOperationTrackerProvider opTrackerFactory,
            ILSMIOOperationSchedulerProvider ioSchedulerProvider, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            double bloomFilterFalsePositiveRate, int version, boolean durable) {
        super(null, mergePolicyFactory, mergePolicyProperties, opTrackerFactory, ioSchedulerProvider,
                ioOpCallbackFactory, bloomFilterFalsePositiveRate, null, null, null, durable);
        this.version = version;
    }

    @Override
    public IIndexDataflowHelper createIndexDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition) {
        return new ExternalBTreeDataflowHelper(opDesc, ctx, partition, bloomFilterFalsePositiveRate,
                mergePolicyFactory.createMergePolicy(mergePolicyProperties, ctx), opTrackerFactory,
                ioSchedulerProvider.getIOScheduler(ctx), ioOpCallbackFactory, false, version, durable);
    }

}
