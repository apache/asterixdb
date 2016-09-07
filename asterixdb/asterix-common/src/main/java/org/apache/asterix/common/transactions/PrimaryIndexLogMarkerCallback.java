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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;

/**
 * A basic callback used to write marker to transaction logs
 */
public class PrimaryIndexLogMarkerCallback implements ILogMarkerCallback {

    private AbstractLSMIndex index;

    /**
     * @param index:
     *            a pointer to the primary index used to store marker log info
     * @throws HyracksDataException
     */
    public PrimaryIndexLogMarkerCallback(AbstractLSMIndex index) throws HyracksDataException {
        this.index = index;
    }

    @Override
    public void before(ByteBuffer buffer) {
        buffer.putLong(index.getCurrentMemoryComponent().getMostRecentMarkerLSN());
    }

    @Override
    public void after(long lsn) {
        index.getCurrentMemoryComponent().setMostRecentMarkerLSN(lsn);
    }
}
