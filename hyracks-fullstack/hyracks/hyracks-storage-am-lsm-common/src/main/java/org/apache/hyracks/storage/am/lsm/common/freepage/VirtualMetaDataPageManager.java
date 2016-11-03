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

package org.apache.hyracks.storage.am.lsm.common.freepage;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IMetaDataPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.api.IVirtualMetaDataPageManager;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class VirtualMetaDataPageManager implements IVirtualMetaDataPageManager {
    protected final int capacity;
    protected final AtomicInteger currentPageId = new AtomicInteger();

    public VirtualMetaDataPageManager(int capacity) {
        // We start the currentPageId from 1, because the BTree uses
        // the first page as metadata page, and the second page as root page.
        // (when returning free pages we first increment, then get)
        currentPageId.set(1);
        this.capacity = capacity;
    }

    @Override
    public int getFreePage(ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {
        // The very first call returns page id 2 because the BTree uses
        // the first page as metadata page, and the second page as root page.
        return currentPageId.incrementAndGet();
    }

    @Override
    public int getFreePageBlock(ITreeIndexMetaDataFrame metaFrame, int count) throws HyracksDataException {
        return currentPageId.getAndUpdate(operand -> operand + count) + 1;
    }

    @Override
    public int getMaxPage(ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {
        return currentPageId.get();
    }

    @Override
    public void init(ITreeIndexMetaDataFrame metaFrame, int currentMaxPage) throws HyracksDataException {
        currentPageId.set(1);
    }

    @Override
    public ITreeIndexMetaDataFrameFactory getMetaDataFrameFactory() {
        return NullMetadataFrameFactory.INSTANCE;
    }

    @Override
    public int getCapacity() {
        return capacity - 2;
    }

    @Override
    public void reset() {
        currentPageId.set(1);
    }

    @Override
    public void addFreePage(ITreeIndexMetaDataFrame metaFrame, int freePage) throws HyracksDataException {
    }

    @Override
    public void addFreePageBlock(ITreeIndexMetaDataFrame metaFrame, int startingPage, int count)
            throws HyracksDataException {
    }

    @Override
    public byte getMetaPageLevelIndicator() {
        return 0;
    }

    @Override
    public byte getFreePageLevelIndicator() {
        return 0;
    }

    @Override
    public boolean isMetaPage(ITreeIndexMetaDataFrame metaFrame) {
        return false;
    }

    @Override
    public boolean isFreePage(ITreeIndexMetaDataFrame metaFrame) {
        return false;
    }

    @Override
    public int getFirstMetadataPage() {
        //MD page in a virtual context is always 0, because it is by nature an in-place modification tree
        return 0;
    }

    @Override
    public void open(int fileId) {
        // Method doesn't make sense for this free page manager.
    }

    @Override
    public void close() {
        // Method doesn't make sense for this free page manager.
    }

    private static class NullMetadataFrameFactory implements ITreeIndexMetaDataFrameFactory {
        private static final NullMetadataFrameFactory INSTANCE = new NullMetadataFrameFactory();

        @Override
        public ITreeIndexMetaDataFrame createFrame() {
            return null;
        }

    }

    @Override
    public void init(ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {
        // Method doesn't make sense for this free page manager.
    }

    @Override
    public int getFilterPageId() throws HyracksDataException {
        // Method doesn't make sense for this free page manager.
        return 0;
    }

    @Override
    public void setFilterPageId(int filterPageId) throws HyracksDataException {
        // Method doesn't make sense for this free page manager.
    }

    @Override
    public long getLSN() throws HyracksDataException {
        // Method doesn't make sense for this free page manager.
        return -1;
    }

    @Override
    public void setLSN(long lsn) throws HyracksDataException {
        // Method doesn't make sense for this free page manager.
    }

    @Override
    public void setFilterPage(ICachedPage page) {
        // Method doesn't make sense for this free page manager.
    }

    @Override
    public ICachedPage getFilterPage() {
        return null;
    }

    @Override
    public boolean appendOnlyMode() {
        return false;
    }

    @Override
    public long getLSNOffset() throws HyracksDataException {
        return IMetaDataPageManager.INVALID_LSN_OFFSET;
    }

    @Override
    public long getLastMarkerLSN() throws HyracksDataException {
        // Method doesn't make sense for this free page manager.
        return -1L;
    }

    @Override
    public void setRootPage(int rootPage) throws HyracksDataException {
        // This won't get called for an in-place index. The root page
        // is maintained at a fixed location as in the below method.
    }

    @Override
    public int getRootPage() throws HyracksDataException {
        // This also won't be called but the correct answer for an
        // In-place index is always 1.
        return 1;
    }
}
