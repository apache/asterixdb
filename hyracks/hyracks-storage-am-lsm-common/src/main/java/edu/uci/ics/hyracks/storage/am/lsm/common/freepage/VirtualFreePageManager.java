/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.lsm.common.freepage;

import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IVirtualFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;

public class VirtualFreePageManager implements IVirtualFreePageManager {
    protected final int capacity;
    protected final AtomicInteger currentPageId = new AtomicInteger();

    public VirtualFreePageManager(int capacity) {
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

    public int getCapacity() {
        return capacity - 2;
    }

    public void reset() {
        currentPageId.set(1);
    }

    @Override
    public void addFreePage(ITreeIndexMetaDataFrame metaFrame, int freePage) throws HyracksDataException {
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
        // Method doesn't make sense for this free page manager.
        return -1;
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
}
