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
package org.apache.asterix.external.api;

import java.io.Closeable;
import java.io.IOException;

import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * This interface represents a record reader that reads data from external source as a set of records
 *
 * @param <T>
 */
public interface IRecordReader<T> extends Closeable {

    /**
     * @return true if the reader has more records remaining, false, otherwise.
     * @throws Exception
     *             if an error takes place
     */
    public boolean hasNext() throws Exception;

    /**
     * @return the object representing the next record.
     * @throws IOException
     * @throws InterruptedException
     */
    public IRawRecord<T> next() throws IOException, InterruptedException;

    /**
     * used to stop reader from producing more records.
     *
     * @return true if the connection to the external source has been suspended, false otherwise.
     */
    public boolean stop();

    // TODO: Find a better way to do flushes, this doesn't fit here
    /**
     * set a pointer to the controller of the feed. the controller can be used to flush()
     * parsed records when waiting for more records to be pushed
     */
    public void setController(AbstractFeedDataFlowController controller);

    // TODO: Find a better way to perform logging. this doesn't fit here
    /**
     * set a pointer to the log manager of the feed. the log manager can be used to log
     * progress and errors
     *
     * @throws HyracksDataException
     */
    public void setFeedLogManager(FeedLogManager feedLogManager) throws HyracksDataException;

    /**
     * gives the record reader a chance to recover from IO errors during feed intake
     */
    public boolean handleException(Throwable th);

    public default IFeedMarker getProgressReporter() {
        return null;
    }

    /**
     * @return JSON String containing ingestion stats
     */
    default String getStats() {
        return null;
    }
}
