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
package org.apache.asterix.metadata.feeds;

import java.util.Map;

import org.apache.asterix.common.active.ActiveJobId;
import org.apache.asterix.common.feeds.api.IActiveRuntime.ActiveRuntimeType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;

public class ProcedureMetaOperatorDescriptor extends ActiveMetaOperatorDescriptor {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public ProcedureMetaOperatorDescriptor(JobSpecification spec, ActiveJobId feedConnectionId,
            IOperatorDescriptor coreOperatorDescriptor, Map<String, String> feedPolicyProperties,
            ActiveRuntimeType runtimeType, String operandId) {
        super(spec, feedConnectionId, coreOperatorDescriptor, feedPolicyProperties, runtimeType, operandId);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        IOperatorNodePushable nodePushable = null;
        switch (runtimeType) {
            case ETS:
                nodePushable = new ProcedureMetaNodePushable(ctx, recordDescProvider, partition, nPartitions,
                        coreOperator, activeJobId, feedPolicyProperties, operandId);
                break;
            case JOIN:
                break;
            default:
                throw new HyracksDataException(new IllegalArgumentException("Invalid active runtime: " + runtimeType));
        }
        return nodePushable;
    }

}
