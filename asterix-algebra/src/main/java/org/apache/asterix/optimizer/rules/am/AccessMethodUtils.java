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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.algebra.operators.physical.ExternalDataLookupPOperator;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.AqlSourceId;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.external.IndexingConstants;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractDataSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExternalDataLookupOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;

/**
 * Static helper functions for rewriting plans using indexes.
 */
public class AccessMethodUtils {

    public static void appendPrimaryIndexTypes(Dataset dataset, IAType itemType, List<Object> target)
            throws IOException, AlgebricksException {
        ARecordType recordType = (ARecordType) itemType;
        List<List<String>> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
        for (List<String> partitioningKey : partitioningKeys) {
            target.add(recordType.getSubFieldType(partitioningKey));
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
            AccessMethodAnalysisContext analysisCtx, IOptimizationContext context,
            IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {
        ILogicalExpression constExpression = null;
        IAType constantExpressionType = null;
        LogicalVariable fieldVar = null;
        ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();
        // One of the args must be a runtime constant, and the other arg must be a variable.
        if (arg1.getExpressionTag() == LogicalExpressionTag.VARIABLE
                && arg2.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            return false;
        }
        if (arg2.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            // The arguments of contains() function are asymmetrical, we can only use index if it is on the first argument
            if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.STRING_CONTAINS) {
                return false;
            }
            IAType expressionType = constantRuntimeResultType(arg1, context, typeEnvironment);
            if (expressionType == null) {
                //Not constant at runtime
                return false;
            }
            constantExpressionType = expressionType;
            constExpression = arg1;
            VariableReferenceExpression varExpr = (VariableReferenceExpression) arg2;
            fieldVar = varExpr.getVariableReference();
        } else if (arg1.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            IAType expressionType = constantRuntimeResultType(arg2, context, typeEnvironment);
            if (expressionType == null) {
                //Not constant at runtime
                return false;
            }
            constantExpressionType = expressionType;
            constExpression = arg2;
            VariableReferenceExpression varExpr = (VariableReferenceExpression) arg1;
            fieldVar = varExpr.getVariableReference();
        } else {
            return false;
        }
        OptimizableFuncExpr newOptFuncExpr = new OptimizableFuncExpr(funcExpr, fieldVar, constExpression,
                constantExpressionType);
        for (IOptimizableFuncExpr optFuncExpr : analysisCtx.matchedFuncExprs) {
            //avoid additional optFuncExpressions in case of a join
            if (optFuncExpr.getFuncExpr().equals(funcExpr))
                return true;
        }
        analysisCtx.matchedFuncExprs.add(newOptFuncExpr);
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
        OptimizableFuncExpr newOptFuncExpr = new OptimizableFuncExpr(funcExpr,
                new LogicalVariable[] { fieldVar1, fieldVar2 }, new ILogicalExpression[0], new IAType[0]);
        for (IOptimizableFuncExpr optFuncExpr : analysisCtx.matchedFuncExprs) {
            //avoid additional optFuncExpressions in case of a join
            if (optFuncExpr.getFuncExpr().equals(funcExpr))
                return true;
        }
        analysisCtx.matchedFuncExprs.add(newOptFuncExpr);
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
                Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(index.getKeyFieldTypes().get(0),
                        index.getKeyFieldNames().get(0), recordType);
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
                    for (int i = 0; i < index.getKeyFieldNames().size(); i++) {
                        Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(
                                index.getKeyFieldTypes().get(i), index.getKeyFieldNames().get(i), recordType);
                        dest.add(keyPairType.first);
                    }
                    break;
                }
                case RTREE: {
                    Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(
                            index.getKeyFieldTypes().get(0), index.getKeyFieldNames().get(0), recordType);
                    IAType keyType = keyPairType.first;
                    IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(keyType.getTypeTag());
                    int numKeys = getNumSecondaryKeys(index, recordType);
                    for (int i = 0; i < numKeys; i++) {
                        dest.add(nestedKeyType);
                    }
                    break;
                }
                case LENGTH_PARTITIONED_NGRAM_INVIX:
                case LENGTH_PARTITIONED_WORD_INVIX:
                default:
                    break;
            }
        }
        // Primary keys.
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            //add primary keys
            try {
                appendExternalRecPrimaryKeys(dataset, dest);
            } catch (AsterixException e) {
                throw new AlgebricksException(e);
            }
        } else {
            List<List<String>> partitioningKeys = DatasetUtils.getPartitioningKeys(dataset);
            for (List<String> partitioningKey : partitioningKeys) {
                try {
                    dest.add(recordType.getSubFieldType(partitioningKey));
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }
            }
        }
    }

    public static void appendSecondaryIndexOutputVars(Dataset dataset, ARecordType recordType, Index index,
            boolean primaryKeysOnly, IOptimizationContext context, List<LogicalVariable> dest)
                    throws AlgebricksException {
        int numPrimaryKeys = 0;
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            numPrimaryKeys = IndexingConstants.getRIDSize(dataset);
        } else {
            numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
        }
        int numSecondaryKeys = getNumSecondaryKeys(index, recordType);
        int numVars = (primaryKeysOnly) ? numPrimaryKeys : numPrimaryKeys + numSecondaryKeys;
        for (int i = 0; i < numVars; i++) {
            dest.add(context.newVar());
        }
    }

    public static List<LogicalVariable> getPrimaryKeyVarsFromSecondaryUnnestMap(Dataset dataset,
            ILogicalOperator unnestMapOp) {
        int numPrimaryKeys;
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            numPrimaryKeys = IndexingConstants.getRIDSize(dataset);
        } else {
            numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
        }
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

    public static List<LogicalVariable> getPrimaryKeyVarsFromPrimaryUnnestMap(Dataset dataset,
            ILogicalOperator unnestMapOp) {
        int numPrimaryKeys = DatasetUtils.getPartitioningKeys(dataset).size();
        List<LogicalVariable> primaryKeyVars = new ArrayList<LogicalVariable>();
        List<LogicalVariable> sourceVars = ((UnnestMapOperator) unnestMapOp).getVariables();
        // Assumes the primary keys are located at the beginning.
        for (int i = 0; i < numPrimaryKeys; i++) {
            primaryKeyVars.add(sourceVars.get(i));
        }
        return primaryKeyVars;
    }

    /**
     * Returns the search key expression which feeds a secondary-index search. If we are optimizing a selection query then this method returns
     * the a ConstantExpression from the first constant value in the optimizable function expression.
     * If we are optimizing a join, then this method returns the VariableReferenceExpression that should feed the secondary index probe.
     *
     * @throws AlgebricksException
     */
    public static Pair<ILogicalExpression, Boolean> createSearchKeyExpr(IOptimizableFuncExpr optFuncExpr,
            OptimizableOperatorSubTree indexSubTree, OptimizableOperatorSubTree probeSubTree)
                    throws AlgebricksException {
        if (probeSubTree == null) {
            // We are optimizing a selection query. Search key is a constant.
            // Type Checking and type promotion is done here
            IAType fieldType = optFuncExpr.getFieldType(0);

            ILogicalExpression constantAtRuntimeExpression = null;
            AsterixConstantValue constantValue = null;
            ATypeTag constantValueTag = null;

            constantAtRuntimeExpression = optFuncExpr.getConstantAtRuntimeExpr(0);

            if (constantAtRuntimeExpression.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                constantValue = (AsterixConstantValue) ((ConstantExpression) constantAtRuntimeExpression).getValue();
            }

            constantValueTag = optFuncExpr.getConstantType(0).getTypeTag();

            // type casting applied?
            boolean typeCastingApplied = false;
            // type casting happened from real (FLOAT, DOUBLE) value -> INT value?
            boolean realTypeConvertedToIntegerType = false;
            AsterixConstantValue replacedConstantValue = null;

            // if the constant type and target type does not match, we do a type conversion
            if (constantValueTag != fieldType.getTypeTag() && constantValue != null) {
                replacedConstantValue = ATypeHierarchy.getAsterixConstantValueFromNumericTypeObject(
                        constantValue.getObject(), fieldType.getTypeTag());
                if (replacedConstantValue != null) {
                    typeCastingApplied = true;
                }

                // To check whether the constant is REAL values, and target field is an INT type field.
                // In this case, we need to change the search parameter. Refer to the caller section for the detail.
                switch (constantValueTag) {
                    case DOUBLE:
                    case FLOAT:
                        switch (fieldType.getTypeTag()) {
                            case INT8:
                            case INT16:
                            case INT32:
                            case INT64:
                                realTypeConvertedToIntegerType = true;
                                break;
                            default:
                                break;
                        }
                    default:
                        break;
                }
            }

            if (typeCastingApplied) {
                return new Pair<ILogicalExpression, Boolean>(new ConstantExpression(replacedConstantValue),
                        realTypeConvertedToIntegerType);
            } else {
                return new Pair<ILogicalExpression, Boolean>(optFuncExpr.getConstantAtRuntimeExpr(0), false);
            }
        } else {
            // We are optimizing a join query. Determine which variable feeds the secondary index.
            if (optFuncExpr.getOperatorSubTree(0) == null || optFuncExpr.getOperatorSubTree(0) == probeSubTree) {
                return new Pair<ILogicalExpression, Boolean>(
                        new VariableReferenceExpression(optFuncExpr.getLogicalVar(0)), false);
            } else {
                return new Pair<ILogicalExpression, Boolean>(
                        new VariableReferenceExpression(optFuncExpr.getLogicalVar(1)), false);
            }
        }
    }

    /**
     * Returns the first expr optimizable by this index.
     */
    public static IOptimizableFuncExpr chooseFirstOptFuncExpr(Index chosenIndex,
            AccessMethodAnalysisContext analysisCtx) {
        List<Pair<Integer, Integer>> indexExprs = analysisCtx.getIndexExprs(chosenIndex);
        int firstExprIndex = indexExprs.get(0).first;
        return analysisCtx.matchedFuncExprs.get(firstExprIndex);
    }

    public static int chooseFirstOptFuncVar(Index chosenIndex, AccessMethodAnalysisContext analysisCtx) {
        List<Pair<Integer, Integer>> indexExprs = analysisCtx.getIndexExprs(chosenIndex);
        return indexExprs.get(0).second;
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
        IFunctionInfo secondaryIndexSearch = FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
        UnnestingFunctionCallExpression secondaryIndexSearchFunc = new UnnestingFunctionCallExpression(
                secondaryIndexSearch, secondaryIndexFuncArgs);
        secondaryIndexSearchFunc.setReturnsUniqueValues(true);
        // This is the operator that jobgen will be looking for. It contains an unnest function that has all necessary arguments to determine
        // which index to use, which variables contain the index-search keys, what is the original dataset, etc.
        UnnestMapOperator secondaryIndexUnnestOp = new UnnestMapOperator(secondaryIndexUnnestVars,
                new MutableObject<ILogicalExpression>(secondaryIndexSearchFunc), secondaryIndexOutputTypes,
                retainInput);
        secondaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
        context.computeAndSetTypeEnvironmentForOperator(secondaryIndexUnnestOp);
        secondaryIndexUnnestOp.setExecutionMode(ExecutionMode.PARTITIONED);
        return secondaryIndexUnnestOp;
    }

    public static UnnestMapOperator createPrimaryIndexUnnestMap(AbstractDataSourceOperator dataSourceOp,
            Dataset dataset, ARecordType recordType, ILogicalOperator inputOp, IOptimizationContext context,
            boolean sortPrimaryKeys, boolean retainInput, boolean retainNull, boolean requiresBroadcast)
                    throws AlgebricksException {
        List<LogicalVariable> primaryKeyVars = AccessMethodUtils.getPrimaryKeyVarsFromSecondaryUnnestMap(dataset,
                inputOp);
        // Optionally add a sort on the primary-index keys before searching the primary index.
        OrderOperator order = null;
        if (sortPrimaryKeys) {
            order = new OrderOperator();
            for (LogicalVariable pkVar : primaryKeyVars) {
                Mutable<ILogicalExpression> vRef = new MutableObject<ILogicalExpression>(
                        new VariableReferenceExpression(pkVar));
                order.getOrderExpressions()
                        .add(new Pair<IOrder, Mutable<ILogicalExpression>>(OrderOperator.ASC_ORDER, vRef));
            }
            // The secondary-index search feeds into the sort.
            order.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
            order.setExecutionMode(ExecutionMode.LOCAL);
            context.computeAndSetTypeEnvironmentForOperator(order);
        }
        // The job gen parameters are transferred to the actual job gen via the UnnestMapOperator's function arguments.
        List<Mutable<ILogicalExpression>> primaryIndexFuncArgs = new ArrayList<Mutable<ILogicalExpression>>();
        BTreeJobGenParams jobGenParams = new BTreeJobGenParams(dataset.getDatasetName(), IndexType.BTREE,
                dataset.getDataverseName(), dataset.getDatasetName(), retainInput, retainNull, requiresBroadcast);
        // Set low/high inclusive to true for a point lookup.
        jobGenParams.setLowKeyInclusive(true);
        jobGenParams.setHighKeyInclusive(true);
        jobGenParams.setLowKeyVarList(primaryKeyVars, 0, primaryKeyVars.size());
        jobGenParams.setHighKeyVarList(primaryKeyVars, 0, primaryKeyVars.size());
        jobGenParams.setIsEqCondition(true);
        jobGenParams.writeToFuncArgs(primaryIndexFuncArgs);
        // Variables and types coming out of the primary-index search.
        List<LogicalVariable> primaryIndexUnnestVars = new ArrayList<LogicalVariable>();
        List<Object> primaryIndexOutputTypes = new ArrayList<Object>();
        // Append output variables/types generated by the primary-index search (not forwarded from input).
        primaryIndexUnnestVars.addAll(dataSourceOp.getVariables());
        try {
            appendPrimaryIndexTypes(dataset, recordType, primaryIndexOutputTypes);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
        // An index search is expressed as an unnest over an index-search function.
        IFunctionInfo primaryIndexSearch = FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
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

    public static ScalarFunctionCallExpression findLOJIsNullFuncInGroupBy(GroupByOperator lojGroupbyOp)
            throws AlgebricksException {
        //find IS_NULL function of which argument has the nullPlaceholder variable in the nested plan of groupby.
        ALogicalPlanImpl subPlan = (ALogicalPlanImpl) lojGroupbyOp.getNestedPlans().get(0);
        Mutable<ILogicalOperator> subPlanRootOpRef = subPlan.getRoots().get(0);
        AbstractLogicalOperator subPlanRootOp = (AbstractLogicalOperator) subPlanRootOpRef.getValue();
        boolean foundSelectNonNull = false;
        ScalarFunctionCallExpression isNullFuncExpr = null;
        AbstractLogicalOperator inputOp = subPlanRootOp;
        while (inputOp != null) {
            if (inputOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
                SelectOperator selectOp = (SelectOperator) inputOp;
                if (selectOp.getCondition().getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    if (((AbstractFunctionCallExpression) selectOp.getCondition().getValue()).getFunctionIdentifier()
                            .equals(AlgebricksBuiltinFunctions.NOT)) {
                        ScalarFunctionCallExpression notFuncExpr = (ScalarFunctionCallExpression) selectOp
                                .getCondition().getValue();
                        if (notFuncExpr.getArguments().get(0).getValue()
                                .getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                            if (((AbstractFunctionCallExpression) notFuncExpr.getArguments().get(0).getValue())
                                    .getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.IS_NULL)) {
                                isNullFuncExpr = (ScalarFunctionCallExpression) notFuncExpr.getArguments().get(0)
                                        .getValue();
                                if (isNullFuncExpr.getArguments().get(0).getValue()
                                        .getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                                    foundSelectNonNull = true;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            inputOp = inputOp.getInputs().size() > 0 ? (AbstractLogicalOperator) inputOp.getInputs().get(0).getValue()
                    : null;
        }

        if (!foundSelectNonNull) {
            throw new AlgebricksException(
                    "Could not find the non-null select operator in GroupByOperator for LEFTOUTERJOIN plan optimization.");
        }
        return isNullFuncExpr;
    }

    public static void resetLOJNullPlaceholderVariableInGroupByOp(AccessMethodAnalysisContext analysisCtx,
            LogicalVariable newNullPlaceholderVaraible, IOptimizationContext context) throws AlgebricksException {

        //reset the null placeholder variable in groupby operator
        ScalarFunctionCallExpression isNullFuncExpr = analysisCtx.getLOJIsNullFuncInGroupBy();
        isNullFuncExpr.getArguments().clear();
        isNullFuncExpr.getArguments().add(
                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(newNullPlaceholderVaraible)));

        //recompute type environment.
        OperatorPropertiesUtil.typeOpRec(analysisCtx.getLOJGroupbyOpRef(), context);
    }

    // New < For external datasets indexing>
    private static void appendExternalRecTypes(Dataset dataset, IAType itemType, List<Object> target) {
        target.add(itemType);
    }

    private static void appendExternalRecPrimaryKeys(Dataset dataset, List<Object> target) throws AsterixException {
        int numPrimaryKeys = IndexingConstants.getRIDSize(dataset);
        for (int i = 0; i < numPrimaryKeys; i++) {
            target.add(IndexingConstants.getFieldType(i));
        }
    }

    private static void writeVarList(List<LogicalVariable> varList, List<Mutable<ILogicalExpression>> funcArgs) {
        Mutable<ILogicalExpression> numKeysRef = new MutableObject<ILogicalExpression>(
                new ConstantExpression(new AsterixConstantValue(new AInt32(varList.size()))));
        funcArgs.add(numKeysRef);
        for (LogicalVariable keyVar : varList) {
            Mutable<ILogicalExpression> keyVarRef = new MutableObject<ILogicalExpression>(
                    new VariableReferenceExpression(keyVar));
            funcArgs.add(keyVarRef);
        }
    }

    private static void addStringArg(String argument, List<Mutable<ILogicalExpression>> funcArgs) {
        Mutable<ILogicalExpression> stringRef = new MutableObject<ILogicalExpression>(
                new ConstantExpression(new AsterixConstantValue(new AString(argument))));
        funcArgs.add(stringRef);
    }

    public static ExternalDataLookupOperator createExternalDataLookupUnnestMap(AbstractDataSourceOperator dataSourceOp,
            Dataset dataset, ARecordType recordType, ILogicalOperator inputOp, IOptimizationContext context,
            Index secondaryIndex, boolean retainInput, boolean retainNull) throws AlgebricksException {
        List<LogicalVariable> primaryKeyVars = AccessMethodUtils.getPrimaryKeyVarsFromSecondaryUnnestMap(dataset,
                inputOp);

        // add a sort on the RID fields before fetching external data.
        OrderOperator order = new OrderOperator();
        for (LogicalVariable pkVar : primaryKeyVars) {
            Mutable<ILogicalExpression> vRef = new MutableObject<ILogicalExpression>(
                    new VariableReferenceExpression(pkVar));
            order.getOrderExpressions()
                    .add(new Pair<IOrder, Mutable<ILogicalExpression>>(OrderOperator.ASC_ORDER, vRef));
        }
        // The secondary-index search feeds into the sort.
        order.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
        order.setExecutionMode(ExecutionMode.LOCAL);
        context.computeAndSetTypeEnvironmentForOperator(order);
        List<Mutable<ILogicalExpression>> externalRIDAccessFuncArgs = new ArrayList<Mutable<ILogicalExpression>>();
        //Add dataverse and dataset to the arguments
        AccessMethodUtils.addStringArg(dataset.getDataverseName(), externalRIDAccessFuncArgs);
        AccessMethodUtils.addStringArg(dataset.getDatasetName(), externalRIDAccessFuncArgs);
        AccessMethodUtils.writeVarList(primaryKeyVars, externalRIDAccessFuncArgs);

        // Variables and types coming out of the external access.
        List<LogicalVariable> externalAccessByRIDVars = new ArrayList<LogicalVariable>();
        List<Object> externalAccessOutputTypes = new ArrayList<Object>();
        // Append output variables/types generated by the data scan (not forwarded from input).
        externalAccessByRIDVars.addAll(dataSourceOp.getVariables());
        appendExternalRecTypes(dataset, recordType, externalAccessOutputTypes);

        IFunctionInfo externalAccessByRID = FunctionUtil.getFunctionInfo(AsterixBuiltinFunctions.EXTERNAL_LOOKUP);
        AbstractFunctionCallExpression externalAccessFunc = new ScalarFunctionCallExpression(externalAccessByRID,
                externalRIDAccessFuncArgs);

        ExternalDataLookupOperator externalLookupOp = new ExternalDataLookupOperator(externalAccessByRIDVars,
                new MutableObject<ILogicalExpression>(externalAccessFunc), externalAccessOutputTypes, retainInput,
                dataSourceOp.getDataSource());
        // Fed by the order operator or the secondaryIndexUnnestOp.
        externalLookupOp.getInputs().add(new MutableObject<ILogicalOperator>(order));

        context.computeAndSetTypeEnvironmentForOperator(externalLookupOp);
        externalLookupOp.setExecutionMode(ExecutionMode.PARTITIONED);

        //set the physical operator
        AqlSourceId dataSourceId = new AqlSourceId(dataset.getDataverseName(), dataset.getDatasetName());
        externalLookupOp.setPhysicalOperator(new ExternalDataLookupPOperator(dataSourceId, dataset, recordType,
                secondaryIndex, primaryKeyVars, false, retainInput, retainNull));
        return externalLookupOp;
    }

    //If the expression is constant at runtime, runturn the type
    public static IAType constantRuntimeResultType(ILogicalExpression expr, IOptimizationContext context,
            IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {
        Set<LogicalVariable> usedVariables = new HashSet<LogicalVariable>();
        expr.getUsedVariables(usedVariables);
        if (usedVariables.size() > 0) {
            return null;
        }
        return (IAType) context.getExpressionTypeComputer().getType(expr, context.getMetadataProvider(),
                typeEnvironment);
    }
}
