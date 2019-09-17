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
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class BloomFilterBulkLoader implements IChainedComponentBulkLoader {

    private final IIndexBulkLoader bulkLoader;

    private boolean endedBloomFilterLoad = false;

    public BloomFilterBulkLoader(IIndexBulkLoader bulkLoader) {
        this.bulkLoader = bulkLoader;
    }

    @Override
    public ITupleReference add(ITupleReference tuple) throws HyracksDataException {
        bulkLoader.add(tuple);
        return tuple;
    }

    @Override
    public ITupleReference delete(ITupleReference tuple) throws HyracksDataException {
        // this ensure deleted keys are also added to the bulkloader
        bulkLoader.add(tuple);
        return tuple;
    }

    @Override
    public void end() throws HyracksDataException {
        if (!endedBloomFilterLoad) {
            bulkLoader.end();
            endedBloomFilterLoad = true;
        }
    }

    @Override
    public void abort() throws HyracksDataException {
        bulkLoader.abort();
    }

    @Override
    public void cleanupArtifacts() throws HyracksDataException {
        if (!endedBloomFilterLoad) {
            bulkLoader.abort();
            endedBloomFilterLoad = true;
        }
    }

    @Override
    public void writeFailed(ICachedPage page, Throwable failure) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasFailed() {
        return bulkLoader.hasFailed();
    }

    @Override
    public Throwable getFailure() {
        return bulkLoader.getFailure();
    }

    @Override
    public void force() throws HyracksDataException {
        bulkLoader.force();
    }
}
