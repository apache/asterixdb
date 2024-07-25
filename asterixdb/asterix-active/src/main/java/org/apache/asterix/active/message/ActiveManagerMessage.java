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
package org.apache.asterix.active.message;

import java.io.Serializable;

import org.apache.asterix.active.ActiveManager;
import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.messaging.CcIdentifiedMessage;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ActiveManagerMessage extends CcIdentifiedMessage implements INcAddressedMessage {
    public enum Kind {
        STOP_ACTIVITY,
        REQUEST_STATS,
        GENERIC_EVENT
    }

    private static final long serialVersionUID = 3L;
    private final Kind kind;
    private final ActiveRuntimeId runtimeId;
    private final Serializable payload;
    private final String desc;

    public ActiveManagerMessage(Kind kind, ActiveRuntimeId runtimeId, Serializable payload, String desc) {
        this.kind = kind;
        this.runtimeId = runtimeId;
        this.payload = payload;
        this.desc = desc;
    }

    public Serializable getPayload() {
        return payload;
    }

    public ActiveRuntimeId getRuntimeId() {
        return runtimeId;
    }

    public Kind getKind() {
        return kind;
    }

    public String getDesc() {
        return desc;
    }

    @Override
    public void handle(INcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        ((ActiveManager) appCtx.getActiveManager()).handle(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{kind=" + kind + ", runtimeId=" + runtimeId
                + (desc != null && !desc.isEmpty() ? ", desc=" + desc : "") + '}';
    }
}
