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
package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexHelperOpenMode;
import edu.uci.ics.hyracks.storage.am.common.utility.TreeIndexStats;
import edu.uci.ics.hyracks.storage.am.common.utility.TreeIndexStatsGatherer;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class BTreeStatsOperatorNodePushable extends
AbstractOperatorNodePushable {
	private final BTreeOpHelper btreeOpHelper;
	private final IHyracksStageletContext ctx;
	private TreeIndexStatsGatherer statsGatherer;

	public BTreeStatsOperatorNodePushable(
			AbstractBTreeOperatorDescriptor opDesc,
			IHyracksStageletContext ctx, int partition) {
		btreeOpHelper = new BTreeOpHelper(opDesc, ctx, partition,
		        IndexHelperOpenMode.CREATE);
		this.ctx = ctx;
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
			btreeOpHelper.init();
			btreeOpHelper.getBTree().open(btreeOpHelper.getBTreeFileId());

			BTree btree = btreeOpHelper.getBTree();			
			IBufferCache bufferCache = btreeOpHelper.getOperatorDescriptor().getStorageManager().getBufferCache(ctx);

			statsGatherer = new TreeIndexStatsGatherer(bufferCache, btree.getFreePageManager(), btreeOpHelper.getBTreeFileId(), btree.getRootPageId());
			TreeIndexStats stats = statsGatherer.gatherStats(btree.getLeafFrameFactory().getFrame(), btree.getInteriorFrameFactory().getFrame(), btree.getFreePageManager().getMetaDataFrameFactory().getFrame());
			System.err.println(stats.toString());
		} catch (Exception e) {
			btreeOpHelper.deinit();
			throw new HyracksDataException(e);
		}
	}

	@Override
	public void setOutputFrameWriter(int index, IFrameWriter writer,
			RecordDescriptor recordDesc) {		
	}
}