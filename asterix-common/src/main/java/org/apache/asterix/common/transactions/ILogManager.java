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

import org.apache.asterix.common.exceptions.ACIDException;

public interface ILogManager {

    /**
     * Submits a logRecord to log Manager which appends it to the log tail
     * @param logRecord
     * @throws ACIDException
     */
    public void log(ILogRecord logRecord) throws ACIDException;

    /**
     * 
     * @param isRecoveryMode
     * @returnLogReader instance which enables reading the log files
     */
    public ILogReader getLogReader(boolean isRecoveryMode);
    
    /**
     * 
     * @return the last LSN the log manager used
     */
    public long getAppendLSN(); 
    
    /**
     * Deletes all log partitions which have a maximum LSN less than checkpointLSN
     * @param checkpointLSN
     */
    public void deleteOldLogFiles(long checkpointLSN);
    
    /**
     * 
     * @return the smallest readable LSN on the current log partitions
     */
    public long getReadableSmallestLSN();

}
