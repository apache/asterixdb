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

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class IndexWithBuddyBulkLoader implements IChainedComponentBulkLoader {

    private final IIndexBulkLoader bulkLoader;
    private final IIndexBulkLoader buddyBTreeBulkLoader;

    public IndexWithBuddyBulkLoader(IIndexBulkLoader bulkLoader, IIndexBulkLoader buddyBTreeBulkLoader) {
        this.bulkLoader = bulkLoader;
        this.buddyBTreeBulkLoader = buddyBTreeBulkLoader;
    }

    @Override
    public ITupleReference delete(ITupleReference tuple) throws HyracksDataException {
        try {
            buddyBTreeBulkLoader.add(tuple);
        } catch (HyracksDataException e) {
            //deleting a key multiple times is OK
            if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                cleanupArtifacts();
                throw e;
            }
        }
        return tuple;
    }

    @Override
    public void cleanupArtifacts() throws HyracksDataException {
        //Noop
    }

    @Override
    public ITupleReference add(ITupleReference tuple) throws HyracksDataException {
        bulkLoader.add(tuple);
        return tuple;
    }

    @Override
    public void end() throws HyracksDataException {
        bulkLoader.end();
        buddyBTreeBulkLoader.end();
    }

    @Override
    public void abort() throws HyracksDataException {
        bulkLoader.abort();
        buddyBTreeBulkLoader.abort();
    }

    @Override
    public void writeFailed(ICachedPage page, Throwable failure) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasFailed() {
        return bulkLoader.hasFailed() || buddyBTreeBulkLoader.hasFailed();
    }

    @Override
    public Throwable getFailure() {
        if (bulkLoader.hasFailed()) {
            return bulkLoader.getFailure();
        } else if (buddyBTreeBulkLoader.hasFailed()) {
            return buddyBTreeBulkLoader.getFailure();
        }
        return null;
    }

    @Override
    public void force() throws HyracksDataException {
        bulkLoader.force();
        buddyBTreeBulkLoader.force();
    }
}
