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
package org.apache.asterix.common.api;

import java.util.Collection;

import org.apache.asterix.common.context.DatasetInfo;
import org.apache.asterix.common.context.DatasetLifecycleManager;
import org.apache.asterix.common.context.IndexInfo;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * An {@link DatasetLifecycleManager#waitForIOAndPerform(IReplicationStrategy, int, IIOBlockingOperation)} operation,
 * which can be executed while the I/O is blocked for each open {@link DatasetInfo}
 */
public interface IIOBlockingOperation {

    /**
     * Prepares for calling {@link #perform(Collection)} on the provided {@code partition}.
     */
    void beforeOperation() throws HyracksDataException;

    /**
     * Performs the required operations. The operation will be performed in a {@code synchronize} block on
     * {@link DatasetInfo}, which would block all operations on the dataset
     *
     * @param indexes to perform the operation against
     * @see DatasetInfo#waitForIOAndPerform(int, IIOBlockingOperation)
     */
    void perform(Collection<IndexInfo> indexes) throws HyracksDataException;

    /**
     * After calling {@link #perform(Collection)}, this should be invoked to perform any necessary clean up
     */
    void afterOperation() throws HyracksDataException;
}
