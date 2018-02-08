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
package org.apache.asterix.runtime.message;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class TxnIdBlockResponse implements INcAddressedMessage {
    private static final long serialVersionUID = 1L;
    private final long startingId;
    private final int blockSize;

    public TxnIdBlockResponse(long startingId, int blockSize) {
        this.startingId = startingId;
        this.blockSize = blockSize;
    }

    public long getStartingId() {
        return startingId;
    }

    public int getBlockSize() {
        return blockSize;
    }

    @Override
    public void handle(INcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        TxnIdBlockRequest.addResponse(this);
    }

    @Override
    public String toString() {
        return TxnIdBlockResponse.class.getSimpleName();
    }
}
