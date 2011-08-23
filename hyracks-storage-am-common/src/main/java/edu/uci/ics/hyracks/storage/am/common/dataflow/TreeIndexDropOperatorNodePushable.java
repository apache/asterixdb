/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.common.dataflow;

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class TreeIndexDropOperatorNodePushable extends
		AbstractOperatorNodePushable {
	private static final Logger LOGGER = Logger
			.getLogger(TreeIndexDropOperatorNodePushable.class.getName());

	private final IHyracksTaskContext ctx;
	private IIndexRegistryProvider<ITreeIndex> treeIndexRegistryProvider;
	private IStorageManagerInterface storageManager;
	private IFileSplitProvider fileSplitProvider;
	private int partition;

	public TreeIndexDropOperatorNodePushable(IHyracksTaskContext ctx,
			IStorageManagerInterface storageManager,
			IIndexRegistryProvider<ITreeIndex> treeIndexRegistryProvider,
			IFileSplitProvider fileSplitProvider, int partition) {
		this.ctx = ctx;
		this.storageManager = storageManager;
		this.treeIndexRegistryProvider = treeIndexRegistryProvider;
		this.fileSplitProvider = fileSplitProvider;
		this.partition = partition;
	}

	@Override
	public void deinitialize() throws HyracksDataException {
	}

	@Override
	public int getInputArity() {
		return 0;
	}

	@Override
	public IFrameWriter getInputFrameWriter(int index) {
		return null;
	}

	@Override
	public void initialize() throws HyracksDataException {
		try {

			IndexRegistry<ITreeIndex> treeIndexRegistry = treeIndexRegistryProvider
					.getRegistry(ctx);
			IBufferCache bufferCache = storageManager.getBufferCache(ctx);
			IFileMapProvider fileMapProvider = storageManager
					.getFileMapProvider(ctx);

			FileReference f = fileSplitProvider.getFileSplits()[partition]
					.getLocalFile();

			boolean fileIsMapped = fileMapProvider.isMapped(f);
			if (!fileIsMapped) {
				throw new HyracksDataException("Cannot drop Tree with name "
						+ f.toString() + ". No file mapping exists.");
			}

			int indexFileId = fileMapProvider.lookupFileId(f);

			// unregister tree instance
			treeIndexRegistry.lock();
			try {
				treeIndexRegistry.unregister(indexFileId);
			} finally {
				treeIndexRegistry.unlock();
			}

			// remove name to id mapping
			bufferCache.deleteFile(indexFileId);
		}
		// TODO: for the time being we don't throw,
		// with proper exception handling (no hanging job problem) we should
		// throw
		catch (Exception e) {
			if (LOGGER.isLoggable(Level.WARNING)) {
				LOGGER.warning("Tree Drop Operator Failed Due To Exception: "
						+ e.getMessage());
			}
		}
	}

	@Override
	public void setOutputFrameWriter(int index, IFrameWriter writer,
			RecordDescriptor recordDesc) {
	}
}