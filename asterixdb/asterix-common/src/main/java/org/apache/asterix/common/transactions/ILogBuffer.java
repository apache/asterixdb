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

import java.nio.channels.FileChannel;

public interface ILogBuffer {

    /**
     * append a log record
     *
     * @param logRecord
     *            the log record to be appended
     * @param appendLsn
     *            the lsn for the record in the log file
     */
    void append(ILogRecord logRecord, long appendLsn);

    /**
     * flush content of buffer to disk
     * @param stopping
     */
    void flush(boolean stopping);

    /**
     * @param logSize
     * @return true if buffer has enough space to append log of size logSize, false otherwise
     */
    boolean hasSpace(int logSize);

    /**
     * Set buffer to be full
     */
    void setFull();

    /**
     * Associate the buffer with a file channel
     *
     * @param fileChannel
     */
    void setFileChannel(FileChannel fileChannel);

    /**
     * reset the buffer for re-use
     */
    void reset();

    /**
     * stops the log buffer
     */
    void stop();

    /**
     * @return the default log page size in this buffer
     */
    int getLogPageSize();
}
