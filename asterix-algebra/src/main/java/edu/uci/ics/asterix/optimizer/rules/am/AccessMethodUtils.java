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

package edu.uci.ics.asterix.optimizer.rules.am;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;

/**
 * Static helper functions for rewriting plans using indexes.
 */
public class AccessMethodUtils {
    public static void appendPrimaryIndexTypes(Dataset dataset, IAType itemType, List<Object> target)
            throws IOException {
        ARecordType recordType = (ARecordType) itemType;
        List<String> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
        for (String partitioningKey : partitioningKeys) {
            target.add(recordType.getFieldType(partitioningKey));
        }
        target.add(itemType);
    }

    public static ConstantExpression createStringConstant(String str) {
        return new ConstantExpression(new AsterixConstantValue(new AString(str)));
    }

    public static ConstantExpression createInt32Constant(int i) {
        return new ConstantExpression(new AsterixConstantValue(new AInt32(i)));
    }

    public static ConstantExpression createBooleanConstant(boolean b) {
        if (b) {
            return new ConstantExpression(new AsterixConstantValue(ABoolean.TRUE));
        } else {
            return new ConstantExpression(new AsterixConstantValue(ABoolean.FALSE));
        }
    }

    public static String getStringConstant(Mutable<ILogicalExpression> expr) {
        IAObject obj = ((AsterixConstantValue) ((ConstantExpression) expr.getValue()).getValue()).getObject();
        return ((AString) obj).getStringValue();
    }

    public static int getInt32Constant(Mutable<ILogicalExpression> expr) {
        IAObject obj = ((AsterixConstantValue) ((ConstantExpression) expr.getValue()).getValue()).getObject();
        return ((AInt32) obj).getIntegerValue();
    }

    public static boolean getBooleanConstant(Mutable<ILogicalExpression> expr) {
        IAObject obj = ((AsterixConstantValue) ((ConstantExpression) expr.getValue()).getValue()).getObject();
        return ((ABoolean) obj).getBoolean();
    }

    public static boolean analyzeFuncExprArgsForOneConstAndVar(AbstractFunctionCallExpression funcExpr,
            AccessMethodAnalysisContext analysisCtx) {
        IAlgebricksConstantValue constFilterVal = null;
        LogicalVariable fieldVar = null;
        ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();
        // One of the args must be a constant, and the other arg must be a variable.
        if (arg1.getExpressionTag() == LogicalExpressionTag.CONSTANT
                && arg2.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            ConstantExpression constExpr = (ConstantExpression) arg1;
            constFilterVal = constExpr.getValue();
            VariableReferenceExpression varExpr = (VariableReferenceExpression) arg2;
            fieldVar = varExpr.getVariableReference();
        } else if (arg1.getExpressionTag() == LogicalExpressionTag.VARIABLE
                && arg2.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            ConstantExpression constExpr = (ConstantExpression) arg2;
            constFilterVal = constExpr.getValue();
            VariableReferenceExpression varExpr = (VariableReferenceExpression) arg1;
            fieldVar = varExpr.getVariableReference();
        } else {
            return false;
        }
        analysisCtx.matchedFuncExprs.add(new OptimizableFuncExpr(funcExpr, fieldVar, constFilterVal));
        return true;
    }

    public static boolean analyzeFuncExprArgsForTwoVars(AbstractFunctionCallExpression funcExpr,
            AccessMethodAnalysisContext analysisCtx) {
        LogicalVariable fieldVar1 = null;
        LogicalVariable fieldVar2 = null;
        ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();
        if (arg1.getExpressionTag() == LogicalExpressionTag.VARIABLE
                && arg2.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            fieldVar1 = ((VariableReferenceExpression) arg1).getVariableReference();
            fieldVar2 = ((VariableReferenceExpression) arg2).getVariableReference();
        } else {
            return false;
        }
        analysisCtx.matchedFuncExprs.add(new OptimizableFuncExpr(funcExpr,
                new LogicalVariable[] { fieldVar1, fieldVar2 }, null));
        return true;
    }

    public static int getNumSecondaryKeys(Index index, ARecordType recordType) throws AlgebricksException {
        switch (index.getIndexType()) {
            case BTREE:
            case SINGLE_PARTITION_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX: 
            case LENGTH_PARTITIONED_WORD_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX: {
                return index.getKeyFieldNames().size();
            }
            case RTREE: {
                Pair<IAType, Boolean> keyPairType = Index.getNonNullableKeyFieldType(index.getKeyFieldNames().get(0),
                        recordType);
                IAType keyType = keyPairType.first;
                int numDimensions = NonTaggedFormatUtil.getNumDimensions(keyType.getTypeTag());
                return numDimensions * 2;
            }
            default: {
                throw new AlgebricksException("Unknown index kind: " + index.getIndexType());
            }
        }
    }

    /**
     * Appends the types of the fields produced by the given secondary index to dest.
     */
    public static void appendSecondaryIndexTypes(Dataset dataset, ARecordType recordType, Index index,
            boolean primaryKeysOnly, List<Object> dest) throws AlgebricksException {
        if (!primaryKeysOnly) {
            switch (index.getIndexType()) {
                case BTREE:
                case SINGLE_PARTITION_WORD_INVIX:
                case SINGLE_PARTITION_NGRAM_INVIX: {
                    for (String sk : index.getKeyFieldNames()) {
                        Pair<IAType, Boolean> keyPairType = Index.getNonNullableKeyFieldType(sk, recordType);
                        dest.add(keyPairType.first);
                    }
                    break;
                }
                case RTREE: {
                    Pair<IAType, Boolean> keyPairType = Index.getNonNullableKeyFieldType(index.getKeyFieldNames()
                            .get(0), recordType);
                    IAType keyType = keyPairType.first;
                    IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(keyType.getTypeTag());
                    int numKeys = getNumSecondaryKeys(index, recordType);
                    for (int i = 0; i < numKeys; i++) {
                        dest.add(nestedKeyType);
                    }
                    break;
                }
            }
        }
        // Primary keys.
        List<String> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
        for (String partitioningKey : partitioningKeys) {
            try {
                dest.add(recordType.getFieldType(partitioningKey));
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
        }
    }

    public static void appendSecondaryIndexOutputVars(Dataset dataset, ARecordType recordType, Index index,
            boolean primaryKeysOnly, IOptimizationContext context, List<LogicalVariable> dest)
            throws AlgebricksException {
        int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
        int numSecondaryKeys = getNumSecondaryKeys(index, recordType);
        int numVars = (primaryKeysOnly) ? numPrimaryKeys : numPrimaryKeys + numSecondaryKeys;
        for (int i = 0; i < numVars; i++) {
            dest.add(context.newVar());
        }
    }

    public static List<LogicalVariable> getPrimaryKeyVarsFromUnnestMap(Dataset dataset, ILogicalOperator unnestMapOp) {
        int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
        List<LogicalVariable> primaryKeyVars = new ArrayList<LogicalVariable>();
        List<LogicalVariable> sourceVars = ((UnnestMapOperator) unnestMapOp).getVariables();
        // Assumes the primary keys are located at the end.
        int start = sourceVars.size() - numPrimaryKeys;
        int stop = sourceVars.size();
        for (int i = start; i < stop; i++) {
            primaryKeyVars.add(sourceVars.get(i));
        }
        return primaryKeyVars;
    }

    /**
     * Returns the search key expression which feeds a secondary-index search. If we are optimizing a selection query then this method returns
     * the a ConstantExpression from the first constant value in the optimizable function expression.
     * If we are optimizing a join, then this method returns the VariableReferenceExpression that should feed the secondary index probe.
     */
    public static ILogicalExpression createSearchKeyExpr(IOptimizableFuncExpr optFuncExpr,
            OptimizableOperatorSubTree indexSubTree, OptimizableOperatorSubTree probeSubTree) {
        if (probeSubTree == null) {
            // We are optimizing a selection query. Search key is a constant.
            return new ConstantExpression(optFuncExpr.getConstantVal(0));
        } else {
            // We are optimizing a join query. Determine which variable feeds the secondary index. 
            if (optFuncExpr.getOperatorSubTree(0) == null || optFuncExpr.getOperatorSubTree(0) == probeSubTree) {
                return new VariableReferenceExpression(optFuncExpr.getLogicalVar(0));
            } else {
                return new VariableReferenceExpression(optFuncExpr.getLogicalVar(1));
            }
        }
    }

    /**
     * Returns the first expr optimizable by this index.
     */
    public static IOptimizableFuncExpr chooseFirstOptFuncExpr(Index chosenIndex, AccessMethodAnalysisContext analysisCtx) {
        List<Integer> indexExprs = analysisCtx.getIndexExprs(chosenIndex);
        int firstExprIndex = indexExprs.get(0);
        return analysisCtx.matchedFuncExprs.get(firstExprIndex);
    }

    public static UnnestMapOperator createSecondaryIndexUnnestMap(Dataset dataset, ARecordType recordType, Index index,
            ILogicalOperator inputOp, AccessMethodJobGenParams jobGenParams, IOptimizationContext context,
            boolean outputPrimaryKeysOnly, boolean retainInput) throws AlgebricksException {
        // The job gen parameters are transferred to the actual job gen via the UnnestMapOperator's function arguments.
        ArrayList<Mutable<ILogicalExpression>> secondaryIndexFuncArgs = new ArrayList<Mutable<ILogicalExpression>>();
        jobGenParams.writeToFuncArgs(secondaryIndexFuncArgs);
        // Variables and types coming out of the secondary-index search. 
        List<LogicalVariable> secondaryIndexUnnestVars = new ArrayList<LogicalVariable>();
        List<Object> secondaryIndexOutputTypes = new ArrayList<Object>();
        // Append output variables/types generated by the secondary-index search (not forwarded from input).
        appendSecondaryIndexOutputVars(dataset, recordType, index, outputPrimaryKeysOnly, context,
                secondaryIndexUnnestVars);
        appendSecondaryIndexTypes(dataset, recordType, index, outputPrimaryKeysOnly, secondaryIndexOutputTypes);
        // An index search is expressed as an unnest over an index-search function.
        IFunctionInfo secondaryIndexSearch = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
        UnnestingFunctionCallExpression secondaryIndexSearchFunc = new UnnestingFunctionCallExpression(
                secondaryIndexSearch, secondaryIndexFuncArgs);
        secondaryIndexSearchFunc.setReturnsUniqueValues(true);
        // This is the operator that jobgen will be looking for. It contains an unnest function that has all necessary arguments to determine
        // which index to use, which variables contain the index-search keys, what is the original dataset, etc.
        UnnestMapOperator secondaryIndexUnnestOp = new UnnestMapOperator(secondaryIndexUnnestVars,
                new MutableObject<ILogicalExpression>(secondaryIndexSearchFunc), secondaryIndexOutputTypes, retainInput);
        secondaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
        context.computeAndSetTypeEnvironmentForOperator(secondaryIndexUnnestOp);
        secondaryIndexUnnestOp.setExecutionMode(ExecutionMode.PARTITIONED);
        return secondaryIndexUnnestOp;
    }

    public static UnnestMapOperator createPrimaryIndexUnnestMap(DataSourceScanOperator dataSourceScan, Dataset dataset,
            ARecordType recordType, ILogicalOperator inputOp, IOptimizationContext context, boolean sortPrimaryKeys,
            boolean retainInput, boolean requiresBroadcast) throws AlgebricksException {
        List<LogicalVariable> primaryKeyVars = AccessMethodUtils.getPrimaryKeyVarsFromUnnestMap(dataset, inputOp);
        // Optionally add a sort on the primary-index keys before searching the primary index.
        OrderOperator order = null;
        if (sortPrimaryKeys) {
            order = new OrderOperator();
            for (LogicalVariable pkVar : primaryKeyVars) {
                Mutable<ILogicalExpression> vRef = new MutableObject<ILogicalExpression>(
                        new VariableReferenceExpression(pkVar));
                order.getOrderExpressions().add(
                        new Pair<IOrder, Mutable<ILogicalExpression>>(OrderOperator.ASC_ORDER, vRef));
            }
            // The secondary-index search feeds into the sort.
            order.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
            order.setExecutionMode(ExecutionMode.LOCAL);
            context.computeAndSetTypeEnvironmentForOperator(order);
        }
        // The job gen parameters are transferred to the actual job gen via the UnnestMapOperator's function arguments. 
        List<Mutable<ILogicalExpression>> primaryIndexFuncArgs = new ArrayList<Mutable<ILogicalExpression>>();
        BTreeJobGenParams jobGenParams = new BTreeJobGenParams(dataset.getDatasetName(), IndexType.BTREE,
                dataset.getDataverseName(), dataset.getDatasetName(), retainInput, requiresBroadcast);
        // Set low/high inclusive to true for a point lookup.
        jobGenParams.setLowKeyInclusive(true);
        jobGenParams.setHighKeyInclusive(true);
        jobGenParams.setLowKeyVarList(primaryKeyVars, 0, primaryKeyVars.size());
        jobGenParams.setHighKeyVarList(primaryKeyVars, 0, primaryKeyVars.size());
        jobGenParams.writeToFuncArgs(primaryIndexFuncArgs);
        // Variables and types coming out of the primary-index search.
        List<LogicalVariable> primaryIndexUnnestVars = new ArrayList<LogicalVariable>();
        List<Object> primaryIndexOutputTypes = new ArrayList<Object>();
        // Append output variables/types generated by the primary-index search (not forwarded from input).
        primaryIndexUnnestVars.addAll(dataSourceScan.getVariables());
        try {
            appendPrimaryIndexTypes(dataset, recordType, primaryIndexOutputTypes);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
        // An index search is expressed as an unnest over an index-search function.
        IFunctionInfo primaryIndexSearch = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
        AbstractFunctionCallExpression primaryIndexSearchFunc = new ScalarFunctionCallExpression(primaryIndexSearch,
                primaryIndexFuncArgs);
        // This is the operator that jobgen will be looking for. It contains an unnest function that has all necessary arguments to determine
        // which index to use, which variables contain the index-search keys, what is the original dataset, etc.
        UnnestMapOperator primaryIndexUnnestOp = new UnnestMapOperator(primaryIndexUnnestVars,
                new MutableObject<ILogicalExpression>(primaryIndexSearchFunc), primaryIndexOutputTypes, retainInput);
        // Fed by the order operator or the secondaryIndexUnnestOp.
        if (sortPrimaryKeys) {
            primaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(order));
        } else {
            primaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
        }
        context.computeAndSetTypeEnvironmentForOperator(primaryIndexUnnestOp);
        primaryIndexUnnestOp.setExecutionMode(ExecutionMode.PARTITIONED);
        return primaryIndexUnnestOp;
    }

}
