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
package org.apache.asterix.common.transactions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class TxnLogFile {

    private final FileChannel fileChannel;
    private final long logFileId;
    private final long fileBeginLSN;
    private final ILogManager logManager;
    private boolean open = true;

    public TxnLogFile(ILogManager logManager, FileChannel fileChannel, long logFileId, long fileBeginLSN) {
        this.logManager = logManager;
        this.fileChannel = fileChannel;
        this.logFileId = logFileId;
        this.fileBeginLSN = fileBeginLSN;
    }

    public void position(long newPosition) throws IOException {
        fileChannel.position(newPosition);
    }

    public long size() throws IOException {
        return fileChannel.size();
    }

    public int read(ByteBuffer readBuffer) throws IOException {
        return fileChannel.read(readBuffer);
    }

    public long getLogFileId() {
        return logFileId;
    }

    public synchronized void close() throws IOException {
        if (open) {
            logManager.closeLogFile(this, fileChannel);
            open = false;
        }
    }

    public long getFileBeginLSN() {
        return fileBeginLSN;
    }
}
