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
package org.apache.asterix.column.common.buffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class TestReadMultiPageOp implements IColumnReadMultiPageOp {
    private final int fileId;
    private final DummyBufferCache dummyBufferCache;
    private final int pageSize;

    public TestReadMultiPageOp(int fileId, DummyBufferCache dummyBufferCache, int pageSize) {
        this.fileId = fileId;
        this.dummyBufferCache = dummyBufferCache;
        this.pageSize = pageSize;
    }

    @Override
    public ICachedPage pin(int pageId) throws HyracksDataException {
        return dummyBufferCache.getBuffer(fileId, pageId);
    }

    @Override
    public void unpin(ICachedPage page) throws HyracksDataException {
        //noop
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }
}
