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
import java.nio.channels.FileChannel;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.replication.IReplicationManager;

public interface ILogManager {

    /**
     * Submits a logRecord to log Manager which appends it to the log tail
     *
     * @param logRecord
     * @throws ACIDException
     */
    public void log(ILogRecord logRecord) throws ACIDException;

    /**
     * @param isRecoveryMode
     * @returnLogReader instance which enables reading the log files
     */
    public ILogReader getLogReader(boolean isRecoveryMode);

    /**
     * @return the last LSN the log manager used
     */
    public long getAppendLSN();

    /**
     * Deletes all log partitions which have a maximum LSN less than checkpointLSN
     *
     * @param checkpointLSN
     */
    public void deleteOldLogFiles(long checkpointLSN);

    /**
     * @return the smallest readable LSN on the current log partitions
     */
    public long getReadableSmallestLSN();

    /**
     * @return The local NC ID
     */
    public String getNodeId();

    /**
     * @return the log page size in bytes
     */
    public int getLogPageSize();

    /**
     * @param replicationManager
     *            the replication manager to be used to replicate logs
     */
    public void setReplicationManager(IReplicationManager replicationManager);

    /**
     * @return the number of log pages
     */
    public int getNumLogPages();

    /**
     * Opens a file channel to the log file which contains {@code LSN}.
     * The start position of the file channel will be at the first LSN of the file.
     *
     * @param LSN
     * @return
     * @throws IOException
     *             if the log file does not exist.
     */
    public TxnLogFile getLogFile(long LSN) throws IOException;

    /**
     * Closes the log file.
     *
     * @param logFileRef
     * @param fileChannel
     * @throws IOException
     */
    public void closeLogFile(TxnLogFile logFileRef, FileChannel fileChannel) throws IOException;

    /**
     * Deletes all current log files and start the next log file partition
     */
    void renewLogFiles();
}
