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

import it.unimi.dsi.fastutil.ints.Int2IntMap;

public class GetEstimatedColumnCountResponseMessage implements INcAddressedMessage {

    private static final long serialVersionUID = 1L;
    private final long reqId;
    private final Int2IntMap columnCount;
    private final Throwable failure;

    public GetEstimatedColumnCountResponseMessage(long reqId, Int2IntMap columnCount, Throwable failure) {
        this.reqId = reqId;
        this.columnCount = columnCount;
        this.failure = failure;
    }

    @Override
    public void handle(INcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        NCMessageBroker mb = (NCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        MessageFuture future = mb.deregisterMessageFuture(reqId);
        if (future != null) {
            future.complete(this);
        }
    }

    public Int2IntMap getColumnCount() {
        return columnCount;
    }

    public Throwable getFailure() {
        return failure;
    }
}
