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
package edu.uci.ics.hyracks.storage.am.common.util;

import java.util.ArrayList;
import java.util.Random;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IntArrayList;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class TreeIndexBufferCacheWarmup {
	private final IBufferCache bufferCache;
	private final IFreePageManager freePageManager;
	private final int fileId;
	private final ArrayList<IntArrayList> pagesByLevel = new ArrayList<IntArrayList>();
	private final Random rnd = new Random();

	public TreeIndexBufferCacheWarmup(IBufferCache bufferCache,
			IFreePageManager freePageManager, int fileId) {
		this.bufferCache = bufferCache;
		this.freePageManager = freePageManager;
		this.fileId = fileId;
	}

	public void warmup(ITreeIndexFrame frame,
			ITreeIndexMetaDataFrame metaFrame, int[] warmupTreeLevels,
			int[] warmupRepeats) throws HyracksDataException {
		bufferCache.openFile(fileId);

		// scan entire file to determine pages in each level
		int maxPageId = freePageManager.getMaxPage(metaFrame);
		for (int pageId = 0; pageId <= maxPageId; pageId++) {
			ICachedPage page = bufferCache.pin(
					BufferedFileHandle.getDiskPageId(fileId, pageId), false);
			page.acquireReadLatch();
			try {
				frame.setPage(page);
				byte level = frame.getLevel();
				while (level >= pagesByLevel.size()) {
					pagesByLevel.add(new IntArrayList(100, 100));
				}
				if (level >= 0) {
					// System.out.println("ADDING: " + level + " " + pageId);
					pagesByLevel.get(level).add(pageId);
				}
			} finally {
				page.releaseReadLatch();
				bufferCache.unpin(page);
			}
		}

		// pin certain pages again to simulate frequent access
		for (int i = 0; i < warmupTreeLevels.length; i++) {
			if (warmupTreeLevels[i] < pagesByLevel.size()) {
				int repeats = warmupRepeats[i];
				IntArrayList pageIds = pagesByLevel.get(warmupTreeLevels[i]);
				int[] remainingPageIds = new int[pageIds.size()];
				for (int r = 0; r < repeats; r++) {
					for (int j = 0; j < pageIds.size(); j++) {
						remainingPageIds[j] = pageIds.get(j);
					}

					int remainingLength = pageIds.size();
					for (int j = 0; j < pageIds.size(); j++) {
						int index = Math.abs(rnd.nextInt()) % remainingLength;
						int pageId = remainingPageIds[index];

						// pin & latch then immediately unlatch & unpin
						ICachedPage page = bufferCache.pin(BufferedFileHandle
								.getDiskPageId(fileId, pageId), false);
						page.acquireReadLatch();
						page.releaseReadLatch();
						bufferCache.unpin(page);

						remainingPageIds[index] = remainingPageIds[remainingLength - 1];
						remainingLength--;
					}
				}
			}
		}

		bufferCache.closeFile(fileId);
	}
}
