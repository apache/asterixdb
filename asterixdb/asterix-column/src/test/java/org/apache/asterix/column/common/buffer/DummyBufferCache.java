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
import java.util.ArrayList;
import java.util.List;

public class DummyBufferCache {
    private final int pageSize;
    private final List<List<DummyPage>> buffers;

    public DummyBufferCache(int pageSize) {
        this.pageSize = pageSize;
        buffers = new ArrayList<>();
    }

    public void clear() {
        buffers.clear();
    }

    public int createFile() {
        int fileId = buffers.size();
        buffers.add(new ArrayList<>());
        return fileId;
    }

    public DummyPage allocate(int fileId) {
        ByteBuffer buffer = ByteBuffer.allocate(pageSize);
        List<DummyPage> filePages = buffers.get(fileId);
        DummyPage page = new DummyPage(buffer, fileId, filePages.size());
        filePages.add(page);
        return page;
    }

    public int getNumberOfBuffers(int fileId) {
        return buffers.get(fileId).size();
    }

    public ByteBuffer allocateTemporary() {
        return ByteBuffer.allocate(pageSize);
    }

    public DummyPage getBuffer(int fileId, int pageId) {
        return buffers.get(fileId).get(pageId);
    }

    public List<DummyPage> duplicate(int fileId, List<DummyPage> pageZeros) {
        int duplicateFileId = buffers.size();
        List<DummyPage> filePages = buffers.get(fileId);
        List<DummyPage> duplicatePages = new ArrayList<>();
        for (DummyPage page : filePages) {
            duplicatePages.add(page.duplicate(duplicateFileId));
        }

        List<DummyPage> duplicatePageZeros = new ArrayList<>();
        for (DummyPage pageZero : pageZeros) {
            duplicatePageZeros.add(duplicatePages.get(pageZero.getPageId()));
        }
        buffers.add(duplicatePages);
        return duplicatePageZeros;
    }
}
