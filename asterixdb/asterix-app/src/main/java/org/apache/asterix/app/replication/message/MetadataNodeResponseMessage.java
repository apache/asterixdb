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
package org.apache.asterix.app.replication.message;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.replication.INCLifecycleMessage;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class MetadataNodeResponseMessage implements INCLifecycleMessage, ICcAddressedMessage {

    private static final long serialVersionUID = 1L;
    private final String nodeId;
    private final boolean exported;

    public MetadataNodeResponseMessage(String nodeId, boolean exported) {
        this.nodeId = nodeId;
        this.exported = exported;
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        appCtx.getNcLifecycleCoordinator().process(this);
    }

    @Override
    public String toString() {
        return MetadataNodeResponseMessage.class.getSimpleName();
    }

    @Override
    public MessageType getType() {
        return MessageType.METADATA_NODE_RESPONSE;
    }

    public boolean isExported() {
        return exported;
    }
}
