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

package org.apache.hyracks.storage.am.common.impls;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.common.buffercache.HaltOnFailureCallback;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractTreeIndex implements ITreeIndex {

    private static final Logger LOGGER = LogManager.getLogger();
    public static final int MINIMAL_TREE_PAGE_COUNT = 2;
    public static final int MINIMAL_TREE_PAGE_COUNT_WITH_FILTER = 3;
    protected int rootPage = 1;

    protected final IBufferCache bufferCache;
    protected final IPageManager freePageManager;

    protected final ITreeIndexFrameFactory interiorFrameFactory;
    protected final ITreeIndexFrameFactory leafFrameFactory;

    protected final IBinaryComparatorFactory[] cmpFactories;
    protected final int fieldCount;

    protected FileReference file;
    private int fileId = -1;

    protected boolean isActive = false;

    protected int bulkloadLeafStart = 0;

    public AbstractTreeIndex(IBufferCache bufferCache, IPageManager freePageManager,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            IBinaryComparatorFactory[] cmpFactories, int fieldCount, FileReference file) {
        this.bufferCache = bufferCache;
        this.freePageManager = freePageManager;
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.cmpFactories = cmpFactories;
        this.fieldCount = fieldCount;
        this.file = file;
    }

    @Override
    public synchronized void create() throws HyracksDataException {
        if (isActive) {
            throw HyracksDataException.create(ErrorCode.CANNOT_CREATE_ACTIVE_INDEX);
        }
        fileId = bufferCache.createFile(file);
        boolean failed = true;
        try {
            bufferCache.openFile(fileId);
            failed = false;
        } finally {
            if (failed) {
                bufferCache.deleteFile(fileId);
            }
        }
        failed = true;
        try {
            freePageManager.open(fileId);
            freePageManager.init(interiorFrameFactory, leafFrameFactory);
            setRootPage();
            freePageManager.close(HaltOnFailureCallback.INSTANCE);
            failed = false;
        } finally {
            bufferCache.closeFile(fileId);
            if (failed) {
                bufferCache.deleteFile(fileId);
            }
        }
    }

    private void setRootPage() throws HyracksDataException {
        rootPage = freePageManager.getRootPageId();
        bulkloadLeafStart = freePageManager.getBulkLoadLeaf();
    }

    @Override
    public synchronized void activate() throws HyracksDataException {
        if (isActive) {
            throw HyracksDataException.create(ErrorCode.CANNOT_ACTIVATE_ACTIVE_INDEX);
        }
        if (fileId >= 0) {
            bufferCache.openFile(fileId);
        } else {
            fileId = bufferCache.openFile(file);
        }
        freePageManager.open(fileId);
        setRootPage();
        // TODO: Should probably have some way to check that the tree is physically consistent
        // or that the file we just opened actually is a tree
        isActive = true;
    }

    @Override
    public synchronized void deactivate() throws HyracksDataException {
        if (!isActive) {
            LOGGER.warn("not deactivating already inactive index {}", this);
            return;
        }
        freePageManager.close(HaltOnFailureCallback.INSTANCE);
        bufferCache.closeFile(fileId);
        isActive = false;
    }

    @Override
    public void purge() throws HyracksDataException {
        if (isActive) {
            throw HyracksDataException.create(ErrorCode.CANNOT_PURGE_ACTIVE_INDEX);
        }
        bufferCache.purgeHandle(fileId);
        // after purging, the fileId has no mapping and no meaning
        fileId = -1;
    }

    @Override
    public synchronized void destroy() throws HyracksDataException {
        if (isActive) {
            throw HyracksDataException.create(ErrorCode.CANNOT_DESTROY_ACTIVE_INDEX);
        }
        bufferCache.deleteFile(file);
    }

    @Override
    public synchronized void clear() throws HyracksDataException {
        if (!isActive) {
            throw HyracksDataException.create(ErrorCode.CANNOT_CLEAR_INACTIVE_INDEX);
        }
        freePageManager.init(interiorFrameFactory, leafFrameFactory);
        setRootPage();
    }

    public boolean isEmptyTree(ITreeIndexFrame frame) throws HyracksDataException {
        if (rootPage == -1) {
            return true;
        }
        return freePageManager.isEmpty(frame, rootPage);
    }

    public byte getTreeHeight(ITreeIndexFrame frame) throws HyracksDataException {
        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), false);
        rootNode.acquireReadLatch();
        try {
            frame.setPage(rootNode);
            return frame.getLevel();
        } finally {
            rootNode.releaseReadLatch();
            bufferCache.unpin(rootNode);
        }
    }

    @Override
    public int getFileId() {
        return fileId;
    }

    public FileReference getFileReference() {
        return file;
    }

    @Override
    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return interiorFrameFactory;
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return leafFrameFactory;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        return cmpFactories;
    }

    @Override
    public IPageManager getPageManager() {
        return freePageManager;
    }

    @Override
    public int getRootPageId() {
        return rootPage;
    }

    @Override
    public int getFieldCount() {
        return fieldCount;
    }

    public IBinaryComparatorFactory[] getCmpFactories() {
        return cmpFactories;
    }

    @Override
    public String toString() {
        return "{\"class\":\"" + getClass().getSimpleName() + "\",\"file\":\"" + file.getRelativePath() + "\"}";
    }
}
