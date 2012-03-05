/*
 * Copyright 2009-2011 by The Regents of the University of California
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

package edu.uci.ics.asterix.runtime.transaction;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.ITransactionManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class TreeIndexInsertUpdateDeleteOperatorDescriptor extends
		AbstractTreeIndexOperatorDescriptor {

	private static final long serialVersionUID = 1L;

	private final int[] fieldPermutation;

	private final IndexOp op;

	private final long transactionId;

	public TreeIndexInsertUpdateDeleteOperatorDescriptor(JobSpecification spec,
			RecordDescriptor recDesc, IStorageManagerInterface storageManager,
			IIndexRegistryProvider<IIndex> treeIndexRegistryProvider,
			IFileSplitProvider fileSplitProvider,
			ITreeIndexFrameFactory interiorFrameFactory,
			ITreeIndexFrameFactory leafFrameFactory, ITypeTraits[] typeTraits,
			IBinaryComparatorFactory[] comparatorFactories,
			IIndexDataflowHelperFactory dataflowHelperFactory,
			int[] fieldPermutation, IndexOp op, long transactionId) {
		super(spec, 1, 1, recDesc, storageManager, treeIndexRegistryProvider,
				fileSplitProvider, interiorFrameFactory, leafFrameFactory,
				typeTraits, comparatorFactories, dataflowHelperFactory);

		this.fieldPermutation = fieldPermutation;
		this.op = op;
		this.transactionId = transactionId; // would obtain it from query
		// context
	}

	@Override
	public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
			IRecordDescriptorProvider recordDescProvider, int partition,
			int nPartitions) {
		TransactionContext txnContext;
		try {
			ITransactionManager transactionManager = ((TransactionProvider) (ctx
					.getJobletContext().getApplicationContext()
					.getApplicationObject())).getTransactionManager();
			txnContext = transactionManager
					.getTransactionContext(transactionId);
		} catch (ACIDException ae) {
			throw new RuntimeException(
					" could not obtain context for invalid transaction id "
							+ transactionId);
		}
		return new TreeIndexInsertUpdateDeleteOperatorNodePushable(txnContext,
				this, ctx, partition, fieldPermutation, recordDescProvider, op);
	}

}
