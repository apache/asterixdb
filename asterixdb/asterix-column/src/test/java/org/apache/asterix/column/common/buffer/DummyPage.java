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

import java.nio.ByteBuffer;

import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class DummyPage extends CachedPage {
    private final ByteBuffer buffer;
    private final int fileId;
    private final int pageId;
    private final long dpid;

    DummyPage(ByteBuffer buffer, int fileId, int pageId) {
        this.buffer = buffer;
        this.fileId = fileId;
        this.pageId = pageId;
        this.dpid = BufferedFileHandle.getDiskPageId(fileId, pageId);
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

    public int getFileId() {
        return fileId;
    }

    public int getPageId() {
        return pageId;
    }

    DummyPage duplicate(int fileId) {
        ByteBuffer duplicate = buffer.duplicate();
        return new DummyPage(duplicate, fileId, pageId);
    }

    @Override
    public long getDiskPageId() {
        return dpid;
    }

    /*
     * **********************************************************
     *  Not used
     * **********************************************************
     */
    @Override
    public void acquireReadLatch() {

    }

    @Override
    public void releaseReadLatch() {

    }

    @Override
    public void acquireWriteLatch() {

    }

    @Override
    public void releaseWriteLatch(boolean markDirty) {

    }

    @Override
    public boolean confiscated() {
        return false;
    }

    @Override
    public int getPageSize() {
        return 0;
    }

    @Override
    public int getFrameSizeMultiplier() {
        return 0;
    }

    @Override
    public void setDiskPageId(long dpid) {

    }

    @Override
    public boolean isLargePage() {
        return false;
    }
}
