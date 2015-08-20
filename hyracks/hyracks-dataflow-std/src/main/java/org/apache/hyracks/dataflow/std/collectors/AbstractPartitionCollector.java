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
package edu.uci.ics.hyracks.dataflow.std.collectors;

import edu.uci.ics.hyracks.api.comm.IPartitionCollector;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.job.JobId;

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