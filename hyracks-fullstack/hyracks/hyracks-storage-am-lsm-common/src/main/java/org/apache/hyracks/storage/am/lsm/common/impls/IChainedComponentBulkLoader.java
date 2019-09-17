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
package org.apache.hyracks.storage.am.lsm.common.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.common.buffercache.IPageWriteFailureCallback;

public interface IChainedComponentBulkLoader extends IPageWriteFailureCallback {
    /**
     * Adds a tuple to the bulkloaded component
     *
     * @param tuple
     * @return Potentially modified tuple, which is used as an input for downstream bulkloaders
     * @throws HyracksDataException
     */
    ITupleReference add(ITupleReference tuple) throws HyracksDataException;

    /**
     * Deletes a tuple (i.e. appends anti-matter tuple or deleted-key tuple) from the bulkloaded component
     *
     * @param tuple
     * @return Potentially modified tuple, which is used as an input for downstream bulkloaders
     * @throws HyracksDataException
     */
    ITupleReference delete(ITupleReference tuple) throws HyracksDataException;

    /**
     * Correctly finalizes bulkloading process and releases all resources
     *
     * @throws HyracksDataException
     */
    void end() throws HyracksDataException;

    /**
     * Aborts bulkloading process without releasing associated resources
     *
     * @throws HyracksDataException
     */
    void abort() throws HyracksDataException;

    /**
     * Releases all resources allocated during the bulkloading process
     *
     * @throws HyracksDataException
     */
    void cleanupArtifacts() throws HyracksDataException;

    /**
     * Force all pages written by this bulkloader to disk.
     * @throws HyracksDataException
     */
    void force() throws HyracksDataException;
}
