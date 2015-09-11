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
package org.apache.hyracks.storage.common.file;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.api.io.IFileHandle;

public class BufferedFileHandle {
    private final int fileId;
    private IFileHandle handle;
    private final AtomicInteger refCount;

    public BufferedFileHandle(int fileId, IFileHandle handle) {
        this.fileId = fileId;
        this.handle = handle;
        refCount = new AtomicInteger();
    }

    public int getFileId() {
        return fileId;
    }

    public IFileHandle getFileHandle() {
        return handle;
    }

    public void markAsDeleted() {
        handle = null;
    }

    public boolean fileHasBeenDeleted() {
        return handle == null;
    }

    public int incReferenceCount() {
        return refCount.incrementAndGet();
    }

    public int decReferenceCount() {
        return refCount.decrementAndGet();
    }

    public int getReferenceCount() {
        return refCount.get();
    }

    public long getDiskPageId(int pageId) {
        return getDiskPageId(fileId, pageId);
    }

    public static long getDiskPageId(int fileId, int pageId) {
        return (((long) fileId) << 32) + pageId;
    }

    public static int getFileId(long dpid) {
        return (int) ((dpid >> 32) & 0xffffffff);
    }

    public static int getPageId(long dpid) {
        return (int) (dpid & 0xffffffff);
    }
}