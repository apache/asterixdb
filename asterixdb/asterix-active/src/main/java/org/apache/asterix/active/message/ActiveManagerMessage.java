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
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.messaging.CcIdentifiedMessage;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ActiveManagerMessage extends CcIdentifiedMessage implements INcAddressedMessage {
    public enum Kind {
        STOP_ACTIVITY,
        REQUEST_STATS
    }

    private static final long serialVersionUID = 1L;
    private final Kind kind;
    private final Serializable payload;

    public ActiveManagerMessage(Kind kind, Serializable payload) {
        this.kind = kind;
        this.payload = payload;
    }

    public Serializable getPayload() {
        return payload;
    }

    public Kind getKind() {
        return kind;
    }

    @Override
    public void handle(INcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        ((ActiveManager) appCtx.getActiveManager()).submit(this);
    }

    @Override
    public String toString() {
        return ActiveManagerMessage.class.getSimpleName();
    }
}
