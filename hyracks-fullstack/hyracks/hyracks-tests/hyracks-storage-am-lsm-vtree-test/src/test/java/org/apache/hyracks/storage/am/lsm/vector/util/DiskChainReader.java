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
package org.apache.hyracks.storage.am.lsm.vector.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTree;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTreeDiskComponent;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeLeafFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeMetadataFrame;
import org.apache.hyracks.storage.am.vector.impls.VTree;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleAccessor;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

/**
 * Reads every data-page chain of every disk component of an {@link LSMVTree} in chain order, for tests
 * that assert on the stored order. Reads the non-quantized layout with a UTF-8 primary key, followed by
 * a UTF-8 INCLUDE field where the layout has one.
 */
public final class DiskChainReader {

    /** One stored entry: where it sits, its ordering key, and its INCLUDE value or null. */
    public record Entry(int pageId, int slot, double distance, String primaryKey, String include) {
        public long distanceBits() {
            return Double.doubleToRawLongBits(distance);
        }
    }

    /** One chain, from its head in {@code nextPage} order. */
    public record Chain(int headPageId, List<Entry> entries) {
    }

    private static final int PK_FIELD = VTreeDataTupleAccessor.NQ_KEY_FIELDS_START;
    private static final int INCLUDE_FIELD = PK_FIELD + 1;
    private static final UTF8StringSerializerDeserializer STRING_SERDE = new UTF8StringSerializerDeserializer();

    private DiskChainReader() {
    }

    public static List<Chain> read(LSMVTree lsmvTree) throws HyracksDataException {
        List<Chain> chains = new ArrayList<>();
        for (ILSMDiskComponent component : lsmvTree.getDiskComponents()) {
            VTree vtree = ((LSMVTreeDiskComponent) component).getIndex();
            for (int directoryPageId : collectDirectoryPages(vtree)) {
                for (int headPageId : chainHeads(vtree, listedDataPages(vtree, directoryPageId))) {
                    chains.add(walkChain(vtree, headPageId));
                }
            }
        }
        return chains;
    }

    /** Every entry of every chain, in chain order. */
    public static List<Entry> entries(List<Chain> chains) {
        List<Entry> all = new ArrayList<>();
        for (Chain chain : chains) {
            all.addAll(chain.entries());
        }
        return all;
    }

    /** The entries carrying {@code primaryKey}, across all chains. */
    public static List<Entry> entriesFor(List<Chain> chains, String primaryKey) {
        List<Entry> found = new ArrayList<>();
        for (Entry entry : entries(chains)) {
            if (primaryKey.equals(entry.primaryKey())) {
                found.add(entry);
            }
        }
        return found;
    }

    /** Every entry that sorts before its predecessor on the same chain, described for an assertion. */
    public static List<String> inversions(List<Chain> chains) {
        List<String> inversions = new ArrayList<>();
        for (Chain chain : chains) {
            Entry previous = null;
            for (Entry entry : chain.entries()) {
                if (previous != null && compareKeys(previous, entry) > 0) {
                    inversions.add("<" + previous.distance() + ", " + previous.primaryKey() + "> precedes <"
                            + entry.distance() + ", " + entry.primaryKey() + "> on page " + entry.pageId() + " slot "
                            + entry.slot());
                }
                previous = entry;
            }
        }
        return inversions;
    }

    /**
     * Every directory whose data pages, read in directory order, are not the chain order from its head.
     * The directory routes a key to the first entry at or above it, so a page listed ahead of its chain
     * predecessor takes keys that belong to that predecessor. A page holding no records is skipped on
     * both sides: a split can empty a page and leave its separator beside the page that took its records.
     */
    public static List<String> directoryOrderMismatches(LSMVTree lsmvTree) throws HyracksDataException {
        List<String> mismatches = new ArrayList<>();
        for (ILSMDiskComponent component : lsmvTree.getDiskComponents()) {
            VTree vtree = ((LSMVTreeDiskComponent) component).getIndex();
            for (int directoryPageId : collectDirectoryPages(vtree)) {
                List<Integer> listed = listedDataPages(vtree, directoryPageId);
                List<Integer> chained = new ArrayList<>();
                for (int headPageId : chainHeads(vtree, listed)) {
                    for (Entry entry : walkChain(vtree, headPageId).entries()) {
                        if (chained.isEmpty() || chained.get(chained.size() - 1) != entry.pageId()) {
                            chained.add(entry.pageId());
                        }
                    }
                }
                List<Integer> listedWithRecords = new ArrayList<>(listed);
                listedWithRecords.retainAll(chained);
                if (!listedWithRecords.equals(chained)) {
                    mismatches.add("directory page " + directoryPageId + " lists " + listedWithRecords
                            + " but the chain runs " + chained);
                }
            }
        }
        return mismatches;
    }

    /**
     * Every entry that repeats its predecessor's key on the same chain. A component holds one entry per
     * key, so a repeat is a replace that appended instead of overwriting.
     */
    public static List<String> duplicateKeys(List<Chain> chains) {
        List<String> duplicates = new ArrayList<>();
        for (Chain chain : chains) {
            Entry previous = null;
            for (Entry entry : chain.entries()) {
                if (previous != null && compareKeys(previous, entry) == 0) {
                    duplicates.add("<" + entry.distance() + ", " + entry.primaryKey() + "> twice on chain "
                            + chain.headPageId() + " (page " + entry.pageId() + " slot " + entry.slot() + ")");
                }
                previous = entry;
            }
        }
        return duplicates;
    }

    /** The page ordering key. The primary keys are ASCII, so string order is their UTF-8 order. */
    private static int compareKeys(Entry left, Entry right) {
        int byDistance = Double.compare(left.distance(), right.distance());
        return byDistance != 0 ? byDistance : left.primaryKey().compareTo(right.primaryKey());
    }

    /** Breadth-first over the static structure, collecting each leaf centroid's directory page. */
    private static List<Integer> collectDirectoryPages(VTree vtree) throws HyracksDataException {
        IBufferCache bufferCache = vtree.getBufferCache();
        int fileId = vtree.getFileId();
        ITreeIndexFrameFactory interiorFrameFactory = vtree.getInteriorFrameFactory();
        ITreeIndexFrameFactory leafFrameFactory = vtree.getLeafFrameFactory();

        List<Integer> directoryPages = new ArrayList<>();
        Queue<Integer> queue = new ArrayDeque<>();
        Set<Integer> visited = new HashSet<>();
        int rootPageId = vtree.getRootPageId();
        queue.add(rootPageId);
        visited.add(rootPageId);

        while (!queue.isEmpty()) {
            int pageId = queue.poll();
            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId));
            page.acquireReadLatch();
            try {
                IVTreeLeafFrame leafFrame = (IVTreeLeafFrame) leafFrameFactory.createFrame();
                leafFrame.setPage(page);
                if (leafFrame.isLeaf()) {
                    for (int i = 0; i < leafFrame.getTupleCount(); i++) {
                        int directoryPageId = leafFrame.getMetadataPagePointer(i);
                        if (directoryPageId > 0) {
                            directoryPages.add(directoryPageId);
                        }
                    }
                    int next = leafFrame.getNextLeaf();
                    if (next > 0 && visited.add(next)) {
                        queue.add(next);
                    }
                } else {
                    IVTreeInteriorFrame interiorFrame = (IVTreeInteriorFrame) interiorFrameFactory.createFrame();
                    interiorFrame.setPage(page);
                    for (int i = 0; i < interiorFrame.getTupleCount(); i++) {
                        int child = interiorFrame.getChildPageId(i);
                        if (child != -1 && visited.add(child)) {
                            queue.add(child);
                        }
                    }
                    if (interiorFrame.getOverflowFlagBit()) {
                        int next = interiorFrame.getNextPage();
                        if (next != -1 && visited.add(next)) {
                            queue.add(next);
                        }
                    }
                }
            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }
        }
        return directoryPages;
    }

    /** The data pages listed by one directory page and its overflow chain, in directory order. */
    private static List<Integer> listedDataPages(VTree vtree, int directoryPageId) throws HyracksDataException {
        IBufferCache bufferCache = vtree.getBufferCache();
        int fileId = vtree.getFileId();
        ITreeIndexFrameFactory metadataFrameFactory = vtree.getMetadataFrameFactory();
        List<Integer> dataPages = new ArrayList<>();
        int pageId = directoryPageId;
        Set<Integer> visitedDirectories = new HashSet<>();
        while (pageId > 0 && visitedDirectories.add(pageId)) {
            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId));
            page.acquireReadLatch();
            try {
                IVTreeMetadataFrame metadataFrame = (IVTreeMetadataFrame) metadataFrameFactory.createFrame();
                metadataFrame.setPage(page);
                for (int i = 0; i < metadataFrame.getTupleCount(); i++) {
                    int dataPageId = metadataFrame.getDataPagePointer(i);
                    if (dataPageId > 0) {
                        dataPages.add(dataPageId);
                    }
                }
                pageId = metadataFrame.getNextPage();
            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }
        }
        return dataPages;
    }

    /**
     * The chain heads among {@code listedDataPages}: the pages no other listed page points at, so a
     * walk starts where the cluster's records start.
     */
    private static List<Integer> chainHeads(VTree vtree, List<Integer> listedDataPages) throws HyracksDataException {
        Set<Integer> pointedAt = new HashSet<>();
        for (int dataPageId : listedDataPages) {
            int next = readNextPage(vtree, dataPageId);
            if (next > 0) {
                pointedAt.add(next);
            }
        }
        List<Integer> heads = new ArrayList<>();
        for (int dataPageId : listedDataPages) {
            if (!pointedAt.contains(dataPageId)) {
                heads.add(dataPageId);
            }
        }
        return heads;
    }

    private static int readNextPage(VTree vtree, int dataPageId) throws HyracksDataException {
        IBufferCache bufferCache = vtree.getBufferCache();
        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(vtree.getFileId(), dataPageId));
        page.acquireReadLatch();
        try {
            IVTreeDataFrame dataFrame = (IVTreeDataFrame) vtree.getDataFrameFactory().createFrame();
            dataFrame.setPage(page);
            return dataFrame.getNextPage();
        } finally {
            page.releaseReadLatch();
            bufferCache.unpin(page);
        }
    }

    private static Chain walkChain(VTree vtree, int headPageId) throws HyracksDataException {
        IBufferCache bufferCache = vtree.getBufferCache();
        int fileId = vtree.getFileId();
        ITreeIndexFrameFactory dataFrameFactory = vtree.getDataFrameFactory();

        List<Entry> entries = new ArrayList<>();
        int pageId = headPageId;
        Set<Integer> visited = new HashSet<>();
        while (pageId > 0 && visited.add(pageId)) {
            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId));
            page.acquireReadLatch();
            try {
                IVTreeDataFrame dataFrame = (IVTreeDataFrame) dataFrameFactory.createFrame();
                dataFrame.setPage(page);
                ITreeIndexTupleReference frameTuple = dataFrame.createTupleReference();
                for (int i = 0; i < dataFrame.getTupleCount(); i++) {
                    frameTuple.resetByTupleIndex(dataFrame, i);
                    String include =
                            frameTuple.getFieldCount() > INCLUDE_FIELD ? readString(frameTuple, INCLUDE_FIELD) : null;
                    entries.add(new Entry(pageId, i, dataFrame.getDistanceToCentroid(i),
                            readString(frameTuple, PK_FIELD), include));
                }
                pageId = dataFrame.getNextPage();
            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }
        }
        return new Chain(headPageId, entries);
    }

    private static String readString(ITreeIndexTupleReference frameTuple, int field) throws HyracksDataException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(frameTuple.getFieldData(field),
                frameTuple.getFieldStart(field), frameTuple.getFieldLength(field)));
        return STRING_SERDE.deserialize(in);
    }
}
