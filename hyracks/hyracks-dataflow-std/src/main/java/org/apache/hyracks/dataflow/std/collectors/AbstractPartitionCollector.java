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
package org.apache.hyracks.dataflow.std.collectors;

import org.apache.hyracks.api.comm.IPartitionCollector;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.job.JobId;

public abstract class AbstractPartitionCollector implements IPartitionCollector {
    protected final IHyracksTaskContext ctx;

    protected final ConnectorDescriptorId connectorId;

    protected final int receiverIndex;

    public AbstractPartitionCollector(IHyracksTaskContext ctx, ConnectorDescriptorId connectorId, int receiverIndex) {
        this.ctx = ctx;
        this.connectorId = connectorId;
        this.receiverIndex = receiverIndex;
    }

    @Override
    public JobId getJobId() {
        return ctx.getJobletContext().getJobId();
    }

    @Override
    public ConnectorDescriptorId getConnectorId() {
        return connectorId;
    }

    @Override
    public int getReceiverIndex() {
        return receiverIndex;
    }
}
