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
package org.apache.asterix.common.storage;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IIndexCheckpointManager {

    /**
     * Initializes the first checkpoint of an index with low watermark {@code lsn}
     *
     * @param lsn
     * @throws HyracksDataException
     */
    void init(long lsn) throws HyracksDataException;

    /**
     * Called when a new LSM disk component is flushed. When called,  the index checkpoiint is updated
     * with the latest valid {@code componentTimestamp} and low watermark {@code lsn}
     *
     * @param componentTimestamp
     * @param lsn
     * @throws HyracksDataException
     */
    void flushed(String componentTimestamp, long lsn) throws HyracksDataException;

    /**
     * Called when a new LSM disk component is replicated from master. When called,  the index checkpoiint is updated
     * with the latest valid {@code componentTimestamp} and the local lsn mapping of {@code masterLsn} is set as the
     * new low watermark.
     *
     * @param componentTimestamp
     * @param masterLsn
     * @throws HyracksDataException
     */
    void replicated(String componentTimestamp, long masterLsn) throws HyracksDataException;

    /**
     * Called when a flush log is received and replicated from master. The mapping between
     * {@code masterLsn} and {@code localLsn} is updated in the checkpoint.
     *
     * @param masterLsn
     * @param localLsn
     * @throws HyracksDataException
     */
    void masterFlush(long masterLsn, long localLsn) throws HyracksDataException;

    /**
     * The index low watermark
     *
     * @return The low watermark
     * @throws HyracksDataException
     */
    long getLowWatermark() throws HyracksDataException;

    /**
     * True if a mapping exists between {@code masterLsn} and a localLsn. Otherwise false.
     *
     * @param masterLsn
     * @return True if the mapping exists. Otherwise false.
     * @throws HyracksDataException
     */
    boolean isFlushed(long masterLsn) throws HyracksDataException;

    /**
     * Advance the index low watermark to {@code lsn}
     *
     * @param lsn
     * @throws HyracksDataException
     */
    void advanceLowWatermark(long lsn) throws HyracksDataException;

    /**
     * Deletes all checkpoints
     */
    void delete();
}
