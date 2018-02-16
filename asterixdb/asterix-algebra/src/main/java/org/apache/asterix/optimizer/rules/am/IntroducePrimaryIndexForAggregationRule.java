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
package org.apache.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Pattern to match in the plan:
 * ...
 * ^
 * |
 * aggregate operator (local)
 * ^
 * |
 * (assign operator)?
 * ^
 * |
 * datasource scan operator OR unnest map operator using the dataset (when WHERE exists on PK)
 * ^
 * |
 * ...
 *
 *
 * The plan is transformed into:
 * ...
 * ^
 * |
 * aggregate operator (local)
 * ^
 * |
 * (assign operator)?
 * ^
 * |
 * unnest map operator over the primary index
 * ^
 * |
 * ...
 * This rule optimizes aggregation queries involving only PKs. It uses the primary index, if present.
 * The primary index is a BTree index that only stores PKs. Therefore, if an aggregation query can be answered by
 * only the PKs, this rule will be fired to use the primary index instead of doing a scan/range search over the dataset.
 */
public class IntroducePrimaryIndexForAggregationRule implements IAlgebraicRewriteRule {
    private final List<Mutable<ILogicalOperator>> parents;

    public IntroducePrimaryIndexForAggregationRule() {
        parents = new ArrayList<>();
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        parents.add(opRef);
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        // remove yourself
        parents.remove(parents.size() - 1);
        // already fired this rule on this operator?
        if (context.checkIfInDontApplySet(this, opRef.getValue())) {
            return false;
        }
        /* only interested in local aggregate operator */
        if (opRef.getValue().getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator localAggregateOperator = (AggregateOperator) opRef.getValue();
        if (localAggregateOperator.isGlobal()) {
            return false;
        }
        context.addToDontApplySet(this, opRef.getValue());
        // find the data scan or unnest map
        Pair<Mutable<ILogicalOperator>, Mutable<ILogicalOperator>> scanAndAssignOpRef =
                findScanAndAssignOperator(localAggregateOperator, context.getMetadataProvider());
        if (scanAndAssignOpRef == null) {
            return false;
        }
        // find its primary index and replace datascan
        boolean transformed = replaceDatascan(localAggregateOperator, scanAndAssignOpRef, context);
        if (transformed) {
            OperatorPropertiesUtil.typeOpRec(opRef, context);
        }
        return transformed;
    }

    private Pair<Mutable<ILogicalOperator>, Mutable<ILogicalOperator>> findScanAndAssignOperator(
            ILogicalOperator localAggregateOperator, IMetadataProvider metadataProvider) throws AlgebricksException {
        Mutable<ILogicalOperator> scanOpRef = localAggregateOperator.getInputs().get(0);
        Mutable<ILogicalOperator> assignOpRef = null;
        // assign operator may or may not exist
        if (scanOpRef.getValue().getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator assignOperator = (AssignOperator) scanOpRef.getValue();
            assignOpRef = new MutableObject<>(assignOperator);
            scanOpRef = scanOpRef.getValue().getInputs().get(0);
        }
        // next operator must be datascan or unnest map using the dataset
        if (scanOpRef.getValue().getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN
                && scanOpRef.getValue().getOperatorTag() != LogicalOperatorTag.UNNEST_MAP) {
            return null;
        }
        if (scanOpRef.getValue().getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
            // for unnest_map, check the index used is the primary index
            UnnestMapOperator unnestMapOperator = (UnnestMapOperator) scanOpRef.getValue();
            ILogicalExpression logicalExpression = unnestMapOperator.getExpressionRef().getValue();
            if (logicalExpression.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return null;
            }
            AbstractFunctionCallExpression functionCallExpression = (AbstractFunctionCallExpression) logicalExpression;
            if (functionCallExpression.getFunctionIdentifier() != BuiltinFunctions.INDEX_SEARCH) {
                return null;
            }
            String indexName = ConstantExpressionUtil.getStringArgument(functionCallExpression, 0);
            String dataverseName = ConstantExpressionUtil.getStringArgument(functionCallExpression, 2);
            String datasetName = ConstantExpressionUtil.getStringArgument(functionCallExpression, 3);
            Index index = ((MetadataProvider) metadataProvider).getIndex(dataverseName, datasetName, indexName);
            if (!index.isPrimaryIndex()) {
                return null;
            }
        }
        return Pair.of(scanOpRef, assignOpRef);
    }

    private boolean replaceDatascan(AggregateOperator localAggregateOperator,
            Pair<Mutable<ILogicalOperator>, Mutable<ILogicalOperator>> scanAndAssignOpRef, IOptimizationContext context)
            throws AlgebricksException {
        /* find the primary index */
        Mutable<ILogicalOperator> scanOperatorRef = scanAndAssignOpRef.getLeft();
        Mutable<ILogicalOperator> assignOperatorRef = scanAndAssignOpRef.getRight();
        AbstractScanOperator scanOperator = (AbstractScanOperator) scanOperatorRef.getValue();
        BTreeJobGenParams originalBTreeParameters = new BTreeJobGenParams();
        Pair<Dataset, Index> datasetAndIndex =
                findDatasetAndSecondaryPrimaryIndex(scanOperator, originalBTreeParameters, context);
        if (datasetAndIndex == null) {
            return false;
        }
        Dataset dataset = datasetAndIndex.getLeft();
        Index primaryIndex = datasetAndIndex.getRight();
        /////// replace the operator. prepare the parameters of the BTree of the new unnestmap operator ///////
        if (dataset.getDatasetType() == DatasetConfig.DatasetType.INTERNAL) {
            /////// check usage of variables produced by scan operator in parents ///////
            Set<LogicalVariable> variablesProducedByScanOp = getVariablesProducedByScanOp(scanOperator,
                    dataset.getPrimaryKeys().size(), scanOperator.getVariables().size());
            boolean variablesAreUsed =
                    scanOperatorVariablesAreUsed(localAggregateOperator, assignOperatorRef, variablesProducedByScanOp);
            if (variablesAreUsed) {
                return false;
            }
            /////// initialize the secondary primary BTree parameters ///////
            boolean retainInput;
            BTreeJobGenParams newBTreeParameters;
            if (scanOperator.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                retainInput = AccessMethodUtils.retainInputs(scanOperator.getVariables(), scanOperator, parents);
                newBTreeParameters = new BTreeJobGenParams(primaryIndex.getIndexName(), DatasetConfig.IndexType.BTREE,
                        dataset.getDataverseName(), dataset.getDatasetName(), retainInput,
                        scanOperator.getInputs().get(0).getValue()
                                .getExecutionMode() == AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
                List<LogicalVariable> empty = new ArrayList<>();
                newBTreeParameters.setLowKeyInclusive(true);
                newBTreeParameters.setHighKeyInclusive(true);
                newBTreeParameters.setIsEqCondition(false);
                newBTreeParameters.setLowKeyVarList(empty, 0, 0);
                newBTreeParameters.setHighKeyVarList(empty, 0, 0);
            } else {
                retainInput = originalBTreeParameters.getRetainInput();
                newBTreeParameters = new BTreeJobGenParams(primaryIndex.getIndexName(), DatasetConfig.IndexType.BTREE,
                        dataset.getDataverseName(), dataset.getDatasetName(), retainInput,
                        originalBTreeParameters.getRequiresBroadcast());
                newBTreeParameters.setLowKeyInclusive(originalBTreeParameters.isLowKeyInclusive());
                newBTreeParameters.setHighKeyInclusive(originalBTreeParameters.isHighKeyInclusive());
                newBTreeParameters.setIsEqCondition(originalBTreeParameters.isEqCondition());
                newBTreeParameters.setLowKeyVarList(originalBTreeParameters.getLowKeyVarList(), 0,
                        originalBTreeParameters.getLowKeyVarList().size());
                newBTreeParameters.setHighKeyVarList(originalBTreeParameters.getHighKeyVarList(), 0,
                        originalBTreeParameters.getHighKeyVarList().size());
            }
            ARecordType recordType = (ARecordType) ((MetadataProvider) context.getMetadataProvider()).findType(dataset);
            ARecordType metaRecordType =
                    (ARecordType) ((MetadataProvider) context.getMetadataProvider()).findMetaType(dataset);
            // create the operator that will replace the dataset scan/search
            AbstractUnnestMapOperator primaryIndexUnnestOperator =
                    (AbstractUnnestMapOperator) AccessMethodUtils.createSecondaryIndexUnnestMap(dataset, recordType,
                            metaRecordType, primaryIndex, scanOperator.getInputs().get(0).getValue(),
                            newBTreeParameters, context, retainInput, false, false);

            // re-use the PK variables of the original scan operator
            primaryIndexUnnestOperator.getVariables().clear();
            for (int i = 0; i < dataset.getPrimaryKeys().size(); i++) {
                primaryIndexUnnestOperator.getVariables().add(scanOperator.getVariables().get(i));
            }
            // now replace
            scanOperatorRef.setValue(primaryIndexUnnestOperator);
            return true;
        }
        return false;
    }

    /**
     * Returns null if there is no primary index defined on the dataset
     * @param scanOperator Scan or unnest-map operator
     * @param originalBTreeParameters The BTree parameters if the operator is unnest-map
     * @param context Needed to get the metadata provider and ask for the index
     * @return The dataset and its primary index
     * @throws AlgebricksException when there is a problem getting the dataset or its indexes from the metadata
     */
    private Pair<Dataset, Index> findDatasetAndSecondaryPrimaryIndex(AbstractScanOperator scanOperator,
            BTreeJobGenParams originalBTreeParameters, IOptimizationContext context) throws AlgebricksException {
        // #1. get the dataset
        Dataset dataset;
        // case 1: dataset scan
        if (scanOperator.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            DataSourceScanOperator dss = (DataSourceScanOperator) scanOperator;
            DataSource ds = (DataSource) dss.getDataSource();
            if (ds.getDatasourceType() != DataSource.Type.INTERNAL_DATASET) {
                return null;
            }
            dataset = ((DatasetDataSource) ds).getDataset();
        } else {
            // case 2: dataset range search
            AbstractFunctionCallExpression primaryIndexFunctionCall =
                    (AbstractFunctionCallExpression) ((UnnestMapOperator) scanOperator).getExpressionRef().getValue();
            originalBTreeParameters.readFromFuncArgs(primaryIndexFunctionCall.getArguments());
            if (originalBTreeParameters.isEqCondition()) {
                return null;
            }
            dataset = ((MetadataProvider) context.getMetadataProvider())
                    .findDataset(originalBTreeParameters.getDataverseName(), originalBTreeParameters.getDatasetName());
        }
        // #2. get all indexes and look for the primary one
        List<Index> indexes = ((MetadataProvider) context.getMetadataProvider())
                .getDatasetIndexes(dataset.getDataverseName(), dataset.getDatasetName());
        for (Index index : indexes) {
            if (index.getKeyFieldNames().isEmpty()) {
                return Pair.of(dataset, index);
            }
        }
        return null;
    }

    private Set<LogicalVariable> getVariablesProducedByScanOp(AbstractScanOperator scanOperator, int startPosition,
            int endPosition) {
        Set<LogicalVariable> variableSet = new HashSet<>();
        // starting after PK, collect the produced variables
        for (int i = startPosition; i < endPosition; i++) {
            variableSet.add(scanOperator.getVariables().get(i));
        }
        return variableSet;
    }

    private boolean scanOperatorVariablesAreUsed(AggregateOperator localAggregateOperator,
            Mutable<ILogicalOperator> assignOperatorRef, Set<LogicalVariable> variablesProducedByScanOp)
            throws AlgebricksException {
        // collect variables used by parents operators
        Set<LogicalVariable> variablesUsedByParents = new HashSet<>();
        for (Mutable<ILogicalOperator> parent : parents) {
            VariableUtilities.getUsedVariables(parent.getValue(), variablesUsedByParents);
        }
        // collect variables used by local aggregate operator
        VariableUtilities.getUsedVariables(localAggregateOperator, variablesUsedByParents);
        // collect variables used by assign operator, if exists
        if (assignOperatorRef != null) {
            VariableUtilities.getUsedVariables(assignOperatorRef.getValue(), variablesUsedByParents);
        }
        // checking...
        for (LogicalVariable producedVariable : variablesProducedByScanOp) {
            if (variablesUsedByParents.contains(producedVariable)) {
                return true;
            }
        }
        return false;
    }
}
