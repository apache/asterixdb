/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.control.nc.comm;

import java.util.UUID;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.ISender;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;

public class Sender implements ISender {
    private final IHyracksStageletContext ctx;

    private final UUID jobId;

    private final int attempt;

    private final UUID stageId;

    private final ConnectorDescriptorId cdId;

    private final int senderIndex;

    public Sender(IHyracksStageletContext ctx, UUID jobId, int attempt, UUID stageId, ConnectorDescriptorId cdId,
            int senderIndex) {
        this.ctx = ctx;
        this.jobId = jobId;
        this.attempt = attempt;
        this.stageId = stageId;
        this.cdId = cdId;
        this.senderIndex = senderIndex;
    }

    @Override
    public IFrameWriter createSenderWriter(int receiverIndex) {
        return null;
    }

    public UUID getJobId() {
        return jobId;
    }

    public int getAttempt() {
        return attempt;
    }

    public UUID getStageId() {
        return stageId;
    }

    public ConnectorDescriptorId getConnectorDescriptorId() {
        return cdId;
    }

    public int getSenderIndex() {
        return senderIndex;
    }
}