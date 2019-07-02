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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;

public interface ICheckpointManager extends ILifeCycleComponent {

    /**
     * @return The latest checkpoint on disk if any exists. Otherwise null.
     */
    Checkpoint getLatest();

    /**
     * Performs a sharp checkpoint.
     *
     * @throws HyracksDataException
     */
    void doSharpCheckpoint() throws HyracksDataException;

    /**
     * Attempts to perform a soft checkpoint at the specified {@code checkpointTargetLSN}.
     *
     * @param checkpointTargetLSN
     * @return The LSN recorded on the captured checkpoint.
     * @throws HyracksDataException
     */
    long tryCheckpoint(long checkpointTargetLSN) throws HyracksDataException;

    /**
     * Secures the current low-water mark until the transaction identified by {@code id} completes.
     *
     * @param id
     * @throws HyracksDataException
     */
    void secure(TxnId id) throws HyracksDataException;

    /**
     * Notifies this {@link ICheckpointManager} that the transaction identified by {@code id} completed.
     *
     * @param id
     */
    void completed(TxnId id);

    /**
     * Checkpoints idle datasets by flushing their in-memory component to disk if needed.
     *
     * @throws HyracksDataException
     */
    void checkpointIdleDatasets() throws HyracksDataException;

    /**
     * Suspends checkpointing datasets
     */
    void suspend();

    /**
     * Resumes checkpointing datasets
     */
    void resume();
}