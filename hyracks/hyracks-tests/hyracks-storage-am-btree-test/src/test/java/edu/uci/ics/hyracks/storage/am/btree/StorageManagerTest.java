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

package edu.uci.ics.hyracks.storage.am.btree;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;

import org.junit.Test;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.util.AbstractBTreeTest;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;
import edu.uci.ics.hyracks.storage.common.sync.LatchType;

public class StorageManagerTest extends AbstractBTreeTest {
    public class PinnedLatchedPage {
        public final ICachedPage page;
        public final LatchType latch;
        public final int pageId;

        public PinnedLatchedPage(ICachedPage page, int pageId, LatchType latch) {
            this.page = page;
            this.pageId = pageId;
            this.latch = latch;
        }
    }

    public enum FileAccessType {
        FTA_READONLY,
        FTA_WRITEONLY,
        FTA_MIXED,
        FTA_UNLATCHED
    }

    public class FileAccessWorker implements Runnable {
        private int workerId;
        private final IBufferCache bufferCache;
        private final int maxPages;
        private final int fileId;
        private final long thinkTime;
        private final int maxLoopCount;
        private final int maxPinnedPages;
        private final int closeFileChance;
        private final FileAccessType fta;
        private int loopCount = 0;
        private boolean fileIsOpen = false;
        private Random rnd = new Random(50);
        private List<PinnedLatchedPage> pinnedPages = new LinkedList<PinnedLatchedPage>();

        public FileAccessWorker(int workerId, IBufferCache bufferCache, FileAccessType fta, int fileId, int maxPages,
                int maxPinnedPages, int maxLoopCount, int closeFileChance, long thinkTime) {
            this.bufferCache = bufferCache;
            this.fileId = fileId;
            this.maxPages = maxPages;
            this.maxLoopCount = maxLoopCount;
            this.maxPinnedPages = maxPinnedPages;
            this.thinkTime = thinkTime;
            this.closeFileChance = closeFileChance;
            this.workerId = workerId;
            this.fta = fta;
        }

        private void pinRandomPage() {
            int pageId = Math.abs(rnd.nextInt() % maxPages);

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(workerId + " PINNING PAGE: " + pageId);
            }

            try {
                ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
                LatchType latch = null;

                switch (fta) {

                    case FTA_UNLATCHED: {
                        latch = null;
                    }
                        break;

                    case FTA_READONLY: {
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info(workerId + " S LATCHING: " + pageId);
                        }
                        page.acquireReadLatch();
                        latch = LatchType.LATCH_S;
                    }
                        break;

                    case FTA_WRITEONLY: {
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info(workerId + " X LATCHING: " + pageId);
                        }
                        page.acquireWriteLatch();
                        latch = LatchType.LATCH_X;
                    }
                        break;

                    case FTA_MIXED: {
                        if (rnd.nextInt() % 2 == 0) {
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info(workerId + " S LATCHING: " + pageId);
                            }
                            page.acquireReadLatch();
                            latch = LatchType.LATCH_S;
                        } else {
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info(workerId + " X LATCHING: " + pageId);
                            }
                            page.acquireWriteLatch();
                            latch = LatchType.LATCH_X;
                        }
                    }
                        break;

                }

                PinnedLatchedPage plPage = new PinnedLatchedPage(page, pageId, latch);
                pinnedPages.add(plPage);
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }
        }

        private void unpinRandomPage() {
            int index = Math.abs(rnd.nextInt() % pinnedPages.size());
            try {
                PinnedLatchedPage plPage = pinnedPages.get(index);

                if (plPage.latch != null) {
                    if (plPage.latch == LatchType.LATCH_S) {
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info(workerId + " S UNLATCHING: " + plPage.pageId);
                        }
                        plPage.page.releaseReadLatch();
                    } else {
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info(workerId + " X UNLATCHING: " + plPage.pageId);
                        }
                        plPage.page.releaseWriteLatch();
                    }
                }
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info(workerId + " UNPINNING PAGE: " + plPage.pageId);
                }

                bufferCache.unpin(plPage.page);
                pinnedPages.remove(index);
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }
        }

        private void openFile() {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(workerId + " OPENING FILE: " + fileId);
            }
            try {
                bufferCache.openFile(fileId);
                fileIsOpen = true;
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }
        }

        private void closeFile() {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(workerId + " CLOSING FILE: " + fileId);
            }
            try {
                bufferCache.closeFile(fileId);
                fileIsOpen = false;
            } catch (HyracksDataException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {

            openFile();

            while (loopCount < maxLoopCount) {
                loopCount++;

                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info(workerId + " LOOP: " + loopCount + "/" + maxLoopCount);
                }

                if (fileIsOpen) {

                    // pin some pages
                    int pagesToPin = Math.abs(rnd.nextInt()) % (maxPinnedPages - pinnedPages.size());
                    for (int i = 0; i < pagesToPin; i++) {
                        pinRandomPage();
                    }

                    // do some thinking
                    try {
                        Thread.sleep(thinkTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    // unpin some pages
                    if (!pinnedPages.isEmpty()) {
                        int pagesToUnpin = Math.abs(rnd.nextInt()) % pinnedPages.size();
                        for (int i = 0; i < pagesToUnpin; i++) {
                            unpinRandomPage();
                        }
                    }

                    // possibly close file
                    int closeFileCheck = Math.abs(rnd.nextInt()) % closeFileChance;
                    if (pinnedPages.isEmpty() || closeFileCheck == 0) {
                        int numPinnedPages = pinnedPages.size();
                        for (int i = 0; i < numPinnedPages; i++) {
                            unpinRandomPage();
                        }
                        closeFile();
                    }
                } else {
                    openFile();
                }
            }

            if (fileIsOpen) {
                int numPinnedPages = pinnedPages.size();
                for (int i = 0; i < numPinnedPages; i++) {
                    unpinRandomPage();
                }
                closeFile();
            }
        }
    }

    @Test
    public void oneThreadOneFileTest() throws Exception {
        IBufferCache bufferCache = harness.getBufferCache();
        bufferCache.createFile(harness.getFileReference());
        int btreeFileId = harness.getFileMapProvider().lookupFileId(harness.getFileReference());
        bufferCache.openFile(btreeFileId);
        Thread worker = new Thread(new FileAccessWorker(0, harness.getBufferCache(), FileAccessType.FTA_UNLATCHED,
                btreeFileId, 10, 10, 100, 10, 0));
        worker.start();
        worker.join();
        bufferCache.closeFile(btreeFileId);
        bufferCache.close();
    }
}
