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
package org.apache.hyracks.storage.am.lsm.vector.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.FieldSubsetTupleProjectorFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunctionFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizerFactory;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;

/**
 * Operator descriptor for vector index search (ANN search).
 * This creates the runtime operator (VTreeSearchOperatorNodePushable) that performs
 * the actual search on each node controller.
 */
public class VTreeSearchOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    // Field indexes in input tuple: [query_vector_field, k_field, metric_field]
    protected final int[] queryFields;

    // Factory to open LSMVTree index
    protected final IIndexDataflowHelperFactory indexHelperFactory;

    // Whether to retain input tuples in output
    protected final boolean retainInput;

    // Transaction callback factory
    protected final ISearchOperationCallbackFactory searchCallbackFactory;

    // Partition mapping (compute nodes to storage nodes)
    protected final int[][] partitionsMap;

    // Data-tuple fields the search emits, in output order
    protected final int[] projectedFields;

    // Tuple projector factory (emits only the projected fields from index results)
    protected final ITupleProjectorFactory tupleProjectorFactory;

    // Factory for creating vector binary accessors (for extracting AOrderedList<ADouble>)
    protected final IVTreeBinaryAccessorFactory vectorAccessorFactory;

    // Factory for creating distance functions, injected from the AsterixDB layer via a Hyracks-side
    // interface to keep this module free of AsterixDB type dependencies.
    protected final IVTreeDistanceFunctionFactory distanceFunctionFactory;

    // Factory for creating per-query quantizers from the float[6] params persisted on the tree.
    // Provided by AsterixDB (OptimizedScalarQuantizerFactory). Nullable for non-quantized indexes.
    protected final IVTreeQuantizerFactory quantizerFactory;

    // Factory for creating tuple filters for INCLUDE field predicates (e.g., year > 2000)
    // When set, the cursor will only return tuples that pass this filter
    protected final ITupleFilterFactory tupleFilterFactory;

    // Physical field indexes, in output order, of the INCLUDE columns that filter reads. The optimizer
    // declares them as output variables of the index search, so the runtime emits them. Empty when no
    // filter was pushed.
    protected final int[] includeFilterFields;

    /** Epsilon from vector index WITH metadata (ANN / cluster search). */
    protected final double indexEpsilon;

    /**
     * Index-only ANN plan flag. When {@code true}, the runtime emits one extra ADOUBLE field per
     * tuple carrying {@code D(q,x)} (read from the cursor via {@code IVectorSearchCursor}), the
     * downstream sort orders on that field, and the primary BTree lookup + rerank ASSIGN are skipped
     * by the optimizer. When {@code false} the legacy lookup-and-rerank path is used.
     */
    protected final boolean indexOnly;

    public VTreeSearchOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor outRecDesc,
            int[] queryFields, IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput,
            ISearchOperationCallbackFactory searchCallbackFactory, IVTreeBinaryAccessorFactory vectorAccessorFactory,
            IVTreeDistanceFunctionFactory distanceFunctionFactory, IVTreeQuantizerFactory quantizerFactory,
            int[][] partitionsMap, int[] projectedFields, ITupleFilterFactory tupleFilterFactory,
            int[] includeFilterFields, double indexEpsilon, boolean indexOnly) {
        super(spec, 1, 1); // 1 input, 1 output
        this.queryFields = queryFields;
        this.indexHelperFactory = indexHelperFactory;
        this.retainInput = retainInput;
        this.searchCallbackFactory = searchCallbackFactory;
        this.vectorAccessorFactory = vectorAccessorFactory;
        this.distanceFunctionFactory = distanceFunctionFactory;
        this.quantizerFactory = quantizerFactory;
        this.partitionsMap = partitionsMap;
        this.projectedFields = projectedFields;
        this.tupleFilterFactory = tupleFilterFactory;
        this.includeFilterFields = includeFilterFields;
        this.indexEpsilon = indexEpsilon;
        this.indexOnly = indexOnly;
        this.outRecDescs[0] = outRecDesc;

        // Emit only the projected fields so the embedding (4KB-16KB per tuple) never reaches an output
        // frame. The caller names them, since it is the layer that knows the data-tuple layout.
        this.tupleProjectorFactory = new FieldSubsetTupleProjectorFactory(projectedFields);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new VTreeSearchOperatorNodePushable(ctx, partition,
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0), queryFields, indexHelperFactory,
                retainInput, searchCallbackFactory, tupleProjectorFactory, vectorAccessorFactory,
                distanceFunctionFactory, quantizerFactory, partitionsMap, tupleFilterFactory, includeFilterFields,
                indexEpsilon, projectedFields.length, indexOnly);
    }
}
