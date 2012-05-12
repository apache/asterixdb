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

import edu.uci.ics.asterix.common.context.AsterixAppRuntimeContext;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.ITransactionManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITupleFilterFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class TreeIndexInsertUpdateDeleteOperatorDescriptor extends AbstractTreeIndexOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final int[] fieldPermutation;

    private final IndexOp op;

    private final long transactionId;

	/*
	 * TODO: Index operators should live in Hyracks. Right now, they are needed
	 * here in Asterix as a hack to provide transactionIDs. The Asterix verions
	 * of this operator will disappear and the operator will come from Hyracks
	 * once the LSM/Recovery/Transactions world has been introduced.
	 */
	public TreeIndexInsertUpdateDeleteOperatorDescriptor(JobSpecification spec,
			RecordDescriptor recDesc, IStorageManagerInterface storageManager,
			IIndexRegistryProvider<IIndex> indexRegistryProvider,
			IFileSplitProvider fileSplitProvider, ITypeTraits[] typeTraits,
			IBinaryComparatorFactory[] comparatorFactories,
			int[] fieldPermutation, IndexOp op,
			IIndexDataflowHelperFactory dataflowHelperFactory,
			ITupleFilterFactory tupleFilterFactory,
			IOperationCallbackProvider opCallbackProvider, long transactionId) {
		super(spec, 1, 1, recDesc, storageManager, indexRegistryProvider,
				fileSplitProvider, typeTraits, comparatorFactories,
				dataflowHelperFactory, tupleFilterFactory, opCallbackProvider);
		this.fieldPermutation = fieldPermutation;
		this.op = op;
		this.transactionId = transactionId;
	}

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        TransactionContext txnContext;
        try {
            ITransactionManager transactionManager = ((AsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext()
                    .getApplicationObject()).getTransactionProvider().getTransactionManager();
            txnContext = transactionManager.getTransactionContext(transactionId);
        } catch (ACIDException ae) {
            throw new RuntimeException(" could not obtain context for invalid transaction id " + transactionId);
        }
        return new TreeIndexInsertUpdateDeleteOperatorNodePushable(txnContext, this, ctx, partition, fieldPermutation,
                recordDescProvider, op);
    }
}
