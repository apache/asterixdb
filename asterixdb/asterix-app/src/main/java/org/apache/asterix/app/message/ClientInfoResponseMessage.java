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
package org.apache.asterix.app.message;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.messaging.NCMessageBroker;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ClientInfoResponseMessage implements INcAddressedMessage {

    private static final long serialVersionUID = 1L;
    private final long reqId;
    private final boolean validRequestId;
    // All time are in millis
    private final long requestCreateTime;
    private final long jobCreateTime;
    private final long jobStartTime;
    private final long jobEndTime;
    private final long jobQueueWaitTime;

    public ClientInfoResponseMessage(long reqId, boolean validRequestId) {
        this(reqId, validRequestId, 0, 0, 0, 0, 0);
    }

    public ClientInfoResponseMessage(long reqId, boolean validRequestId, long requestCreateTime, long jobCreateTime,
            long jobStartTime, long jobEndTime, long jobQueueWaitTime) {
        this.reqId = reqId;
        this.validRequestId = validRequestId;
        this.requestCreateTime = requestCreateTime;
        this.jobStartTime = jobStartTime;
        this.jobCreateTime = jobCreateTime;
        this.jobEndTime = jobEndTime;
        this.jobQueueWaitTime = jobQueueWaitTime;
    }

    @Override
    public void handle(INcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        NCMessageBroker mb = (NCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        MessageFuture future = mb.deregisterMessageFuture(reqId);
        if (future != null) {
            future.complete(this);
        }
    }

    public boolean isValidRequestId() {
        return validRequestId;
    }

    public long getRequestCreateTime() {
        return requestCreateTime;
    }

    public long getJobCreateTime() {
        return jobCreateTime;
    }

    public long getJobStartTime() {
        return jobStartTime;
    }

    public long getJobEndTime() {
        return jobEndTime;
    }

    public long getJobQueueWaitTime() {
        return jobQueueWaitTime;
    }
}
