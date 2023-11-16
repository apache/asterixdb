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
package org.apache.asterix.algebra.operators.physical;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.cluster.PartitioningProperties;
import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.dataset.DatasetFormatInfo;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.DataSourceIndex;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.IndexUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.optimizer.rules.am.BTreeJobGenParams;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.DefaultProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import org.apache.hyracks.algebricks.core.algebra.metadata.IProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.BroadcastPartitioningProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty.PropertyType;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningRequirementsCoordinator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.LocalOrderProperty;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import org.apache.hyracks.algebricks.core.algebra.properties.StructuralPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.UnorderedPartitionedProperty;
import org.apache.hyracks.algebricks.core.jobgen.impl.JobGenContext;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.impls.DefaultTupleProjectorFactory;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeBatchPointSearchCursor;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;

/**
 * Contributes the runtime operator for an unnest-map representing a BTree search.
 */
public class BTreeSearchPOperator extends IndexSearchPOperator {

    private final List<LogicalVariable> lowKeyVarList;
    private final List<LogicalVariable> highKeyVarList;
    private final boolean isPrimaryIndex;
    private final boolean isEqCondition;

    public BTreeSearchPOperator(IDataSourceIndex<String, DataSourceId> idx, INodeDomain domain,
            boolean requiresBroadcast, boolean isPrimaryIndex, boolean isEqCondition,
            List<LogicalVariable> lowKeyVarList, List<LogicalVariable> highKeyVarList) {
        super(idx, domain, requiresBroadcast);
        this.isPrimaryIndex = isPrimaryIndex;
        this.isEqCondition = isEqCondition;
        this.lowKeyVarList = lowKeyVarList;
        this.highKeyVarList = highKeyVarList;
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.BTREE_SEARCH;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        AbstractUnnestMapOperator unnestMap = (AbstractUnnestMapOperator) op;
        ILogicalExpression unnestExpr = unnestMap.getExpressionRef().getValue();
        if (unnestExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            throw new IllegalStateException();
        }
        AbstractFunctionCallExpression unnestFuncExpr = (AbstractFunctionCallExpression) unnestExpr;
        FunctionIdentifier funcIdent = unnestFuncExpr.getFunctionIdentifier();
        if (!funcIdent.equals(BuiltinFunctions.INDEX_SEARCH)) {
            return;
        }
        BTreeJobGenParams jobGenParams = new BTreeJobGenParams();
        jobGenParams.readFromFuncArgs(unnestFuncExpr.getArguments());
        int[] lowKeyIndexes = getKeyIndexes(jobGenParams.getLowKeyVarList(), inputSchemas);
        int[] highKeyIndexes = getKeyIndexes(jobGenParams.getHighKeyVarList(), inputSchemas);

        boolean propagateFilter = unnestMap.propagateIndexFilter();
        IMissingWriterFactory nonFilterWriterFactory = getNonFilterWriterFactory(propagateFilter, context);
        int[] minFilterFieldIndexes = getKeyIndexes(unnestMap.getMinFilterVars(), inputSchemas);
        int[] maxFilterFieldIndexes = getKeyIndexes(unnestMap.getMaxFilterVars(), inputSchemas);

        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        Dataset dataset = metadataProvider.findDataset(jobGenParams.getDatabaseName(), jobGenParams.getDataverseName(),
                jobGenParams.getDatasetName());
        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(op);
        ITupleFilterFactory tupleFilterFactory = null;
        long outputLimit = -1;
        boolean retainMissing = false;
        IMissingWriterFactory nonMatchWriterFactory = null;
        IProjectionFiltrationInfo projectionFiltrationInfo = DefaultProjectionFiltrationInfo.INSTANCE;
        switch (unnestMap.getOperatorTag()) {
            case UNNEST_MAP:
                UnnestMapOperator unnestMapOp = (UnnestMapOperator) unnestMap;
                projectionFiltrationInfo = unnestMapOp.getProjectionFiltrationInfo();
                outputLimit = unnestMapOp.getOutputLimit();
                if (unnestMapOp.getSelectCondition() != null) {
                    tupleFilterFactory = metadataProvider.createTupleFilterFactory(new IOperatorSchema[] { opSchema },
                            typeEnv, unnestMapOp.getSelectCondition().getValue(), context);
                }
                break;
            case LEFT_OUTER_UNNEST_MAP:
                LeftOuterUnnestMapOperator outerUnnestMapOperator = (LeftOuterUnnestMapOperator) unnestMap;
                projectionFiltrationInfo = outerUnnestMapOperator.getProjectionFiltrationInfo();
                // By nature, LEFT_OUTER_UNNEST_MAP should generate missing (or null) values for non-matching tuples.
                retainMissing = true;
                nonMatchWriterFactory =
                        getNonMatchWriterFactory(((LeftOuterUnnestMapOperator) unnestMap).getMissingValue(), context,
                                unnestMap.getSourceLocation());
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, unnestMap.getSourceLocation(),
                        String.valueOf(unnestMap.getOperatorTag()));
        }

        ITupleProjectorFactory tupleProjectorFactory = DefaultTupleProjectorFactory.INSTANCE;
        DatasetFormatInfo formatInfo = dataset.getDatasetFormatInfo();
        if (isPrimaryIndex && formatInfo.getFormat() == DatasetConfig.DatasetFormat.COLUMN) {
            ARecordType datasetType = (ARecordType) metadataProvider.findType(dataset);
            ARecordType metaItemType = (ARecordType) metadataProvider.findMetaType(dataset);
            datasetType =
                    (ARecordType) metadataProvider.findTypeForDatasetWithoutType(datasetType, metaItemType, dataset);
            tupleProjectorFactory = IndexUtil.createTupleProjectorFactory(context, typeEnv, formatInfo,
                    projectionFiltrationInfo, datasetType, metaItemType, dataset.getPrimaryKeys().size());
        }

        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> btreeSearch = metadataProvider.getBtreeSearchRuntime(
                builder.getJobSpec(), opSchema, typeEnv, context, jobGenParams.getRetainInput(), retainMissing,
                nonMatchWriterFactory, dataset, jobGenParams.getIndexName(), lowKeyIndexes, highKeyIndexes,
                jobGenParams.isLowKeyInclusive(), jobGenParams.isHighKeyInclusive(), propagateFilter,
                nonFilterWriterFactory, minFilterFieldIndexes, maxFilterFieldIndexes, tupleFilterFactory, outputLimit,
                unnestMap.getGenerateCallBackProceedResultVar(),
                useBatchPointSearch(op, context.getPhysicalOptimizationConfig()), tupleProjectorFactory,
                isPrimaryIndexPointSearch());
        IOperatorDescriptor opDesc = btreeSearch.first;
        opDesc.setSourceLocation(unnestMap.getSourceLocation());

        builder.contributeHyracksOperator(unnestMap, opDesc);
        builder.contributeAlgebricksPartitionConstraint(opDesc, btreeSearch.second);

        ILogicalOperator srcExchange = unnestMap.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, unnestMap, 0);
    }

    private boolean isPrimaryIndexPointSearch() {
        if (!isEqCondition || !isPrimaryIndex || !lowKeyVarList.equals(highKeyVarList)) {
            return false;
        }
        Index searchIndex = ((DataSourceIndex) idx).getIndex();
        int numberOfKeyFields = ((Index.ValueIndexDetails) searchIndex.getIndexDetails()).getKeyFieldNames().size();
        return lowKeyVarList.size() == numberOfKeyFields && highKeyVarList.size() == numberOfKeyFields;
    }

    /**
     * Check whether we can use {@link LSMBTreeBatchPointSearchCursor} to perform point-lookups on the primary index
     */
    private boolean useBatchPointSearch(ILogicalOperator op, PhysicalOptimizationConfig config) {
        if (!config.isBatchLookupEnabled() || !isPrimaryIndexPointSearch()) {
            return false;
        }

        IPhysicalPropertiesVector vector = op.getInputs().get(0).getValue().getDeliveredPhysicalProperties();
        if (vector != null) {
            for (ILocalStructuralProperty property : vector.getLocalProperties()) {
                if (property.getPropertyType() == PropertyType.LOCAL_ORDER_PROPERTY) {
                    LocalOrderProperty orderProperty = (LocalOrderProperty) property;
                    if (orderProperty.getColumns().equals(lowKeyVarList)
                            && orderProperty.getOrders().stream().allMatch(o -> o.equals(OrderKind.ASC))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent, IOptimizationContext context) throws AlgebricksException {
        if (requiresBroadcast) {
            // For primary indexes optimizing an equality condition we can reduce the broadcast requirement to hash partitioning.
            if (isPrimaryIndex && isEqCondition) {

                // If this is a composite primary index, then all of the keys should be provided.
                Index searchIndex = ((DataSourceIndex) idx).getIndex();
                int numberOfKeyFields =
                        ((Index.ValueIndexDetails) searchIndex.getIndexDetails()).getKeyFieldNames().size();

                if (numberOfKeyFields < 2
                        || (lowKeyVarList.size() == numberOfKeyFields && highKeyVarList.size() == numberOfKeyFields)) {
                    StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
                    ListSet<LogicalVariable> searchKeyVars = new ListSet<>();
                    searchKeyVars.addAll(lowKeyVarList);
                    searchKeyVars.addAll(highKeyVarList);
                    // Also, add a local sorting property to enforce a sort before the primary-index operator.
                    List<ILocalStructuralProperty> propsLocal = new ArrayList<>();
                    List<OrderColumn> orderColumns = new ArrayList<>();
                    for (LogicalVariable orderVar : searchKeyVars) {
                        orderColumns.add(new OrderColumn(orderVar, OrderKind.ASC));
                    }
                    propsLocal.add(new LocalOrderProperty(orderColumns));
                    MetadataProvider mp = (MetadataProvider) context.getMetadataProvider();
                    Dataset dataset = mp.findDataset(searchIndex.getDatabaseName(), searchIndex.getDataverseName(),
                            searchIndex.getDatasetName());
                    PartitioningProperties partitioningProperties = mp.getPartitioningProperties(dataset);
                    pv[0] = new StructuralPropertiesVector(UnorderedPartitionedProperty.ofPartitionsMap(searchKeyVars,
                            domain, partitioningProperties.getComputeStorageMap()), propsLocal);
                    return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
                }
            }
            StructuralPropertiesVector[] pv = new StructuralPropertiesVector[1];
            pv[0] = new StructuralPropertiesVector(new BroadcastPartitioningProperty(domain), null);
            return new PhysicalRequirements(pv, IPartitioningRequirementsCoordinator.NO_COORDINATION);
        } else {
            return super.getRequiredPropertiesForChildren(op, reqdByParent, context);
        }
    }
}
