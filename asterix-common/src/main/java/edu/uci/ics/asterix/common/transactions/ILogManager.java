/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.common.transactions;

import edu.uci.ics.asterix.common.exceptions.ACIDException;

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
