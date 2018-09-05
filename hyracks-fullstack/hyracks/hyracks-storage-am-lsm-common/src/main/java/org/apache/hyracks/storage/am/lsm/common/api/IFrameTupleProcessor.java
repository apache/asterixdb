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
package org.apache.hyracks.storage.am.lsm.common.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface IFrameTupleProcessor {

    /**
     * Called once per batch before starting the batch process
     */
    void start() throws HyracksDataException;

    /**
     * process the frame tuple with the frame index
     *
     * @param tuple
     *            the tuple
     * @param index
     *            the index of the tuple in the frame
     * @throws HyracksDataException
     */
    void process(ITupleReference tuple, int index) throws HyracksDataException;

    /**
     * Called once per batch before ending the batch process
     */
    void finish() throws HyracksDataException;

    /**
     * Called when a failure is encountered processing a frame
     *
     * @param th
     */
    void fail(Throwable th);
}
