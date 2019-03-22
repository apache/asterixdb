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
package org.apache.asterix.replication.sync;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.ReplicationException;
import org.apache.asterix.replication.api.PartitionReplica;
import org.apache.asterix.replication.management.NetworkingUtil;
import org.apache.asterix.replication.messaging.DeleteFileTask;
import org.apache.asterix.replication.messaging.ReplicateFileTask;
import org.apache.asterix.replication.messaging.ReplicationProtocol;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.network.ISocketChannel;

public class FileSynchronizer {

    private final INcApplicationContext appCtx;
    private final PartitionReplica replica;

    public FileSynchronizer(INcApplicationContext appCtx, PartitionReplica replica) {
        this.appCtx = appCtx;
        this.replica = replica;
    }

    public void replicate(String file) {
        replicate(file, false);
    }

    public void replicate(String file, boolean metadata) {
        try {
            final IIOManager ioManager = appCtx.getIoManager();
            final ISocketChannel channel = replica.getChannel();
            final FileReference filePath = ioManager.resolve(file);
            ReplicateFileTask task = new ReplicateFileTask(file, filePath.getFile().length(), metadata);
            ReplicationProtocol.sendTo(replica, task);
            // send the file itself
            try (RandomAccessFile fromFile = new RandomAccessFile(filePath.getFile(), "r");
                    FileChannel fileChannel = fromFile.getChannel()) {
                NetworkingUtil.sendFile(fileChannel, channel);
            }
            ReplicationProtocol.waitForAck(replica);
        } catch (IOException e) {
            throw new ReplicationException(e);
        }
    }

    public void delete(String file) {
        try {
            final DeleteFileTask task = new DeleteFileTask(file);
            ReplicationProtocol.sendTo(replica, task);
            ReplicationProtocol.waitForAck(replica);
        } catch (IOException e) {
            throw new ReplicationException(e);
        }
    }
}
