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

import org.apache.asterix.algebra.operators.physical.ExternalDataLookupPOperator;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.ExternalDatasetDetails;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.KeyFieldTypeUtil;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.typecomputer.base.TypeCastUtils;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizer;

/**
 * Static helper functions for rewriting plans using indexes.
 */
public class AccessMethodUtils {

    public static void appendPrimaryIndexTypes(Dataset dataset, IAType itemType, IAType metaItemType,
            List<Object> target) throws AlgebricksException {
        ARecordType recordType = (ARecordType) itemType;
        ARecordType metaRecordType = (ARecordType) metaItemType;
        target.addAll(KeyFieldTypeUtil.getPartitoningKeyTypes(dataset, recordType, metaRecordType));
        // Adds data record type.
        target.add(itemType);
        // Adds meta record type if any.
        if (dataset.hasMetaPart()) {
            target.add(metaItemType);
        }
    }

    public static ConstantExpression createStringConstant(String str) {
        return new ConstantExpression(new AsterixConstantValue(new AString(str)));
    }

    public static ConstantExpression createInt32Constant(int i) {
        return new ConstantExpression(new AsterixConstantValue(new AInt32(i)));
    }

    public static ConstantExpression createBooleanConstant(boolean b) {
        return new ConstantExpression(new AsterixConstantValue(ABoolean.valueOf(b)));
    }

    public static String getStringConstant(Mutable<ILogicalExpression> expr) {
        return ConstantExpressionUtil.getStringConstant(expr.getValue());
    }

    public static int getInt32Constant(Mutable<ILogicalExpression> expr) {
        return ConstantExpressionUtil.getIntConstant(expr.getValue());
    }

    public static long getInt64Constant(Mutable<ILogicalExpression> expr) {
        return ConstantExpressionUtil.getLongConstant(expr.getValue());
    }

    public static boolean getBooleanConstant(Mutable<ILogicalExpression> expr) {
        return ConstantExpressionUtil.getBooleanConstant(expr.getValue());
    }

    public static boolean analyzeFuncExprArgsForOneConstAndVarAndUpdateAnalysisCtx(
            AbstractFunctionCallExpression funcExpr, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {
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
            if (funcExpr.getFunctionIdentifier() == BuiltinFunctions.STRING_CONTAINS
                    || funcExpr.getFunctionIdentifier() == BuiltinFunctions.FULLTEXT_CONTAINS
                    || funcExpr.getFunctionIdentifier() == BuiltinFunctions.FULLTEXT_CONTAINS_WO_OPTION) {
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

            // For a full-text search query, if the given predicate is a constant and not a single keyword,
            // i.e. it's a phrase, then we currently throw an exception since we don't support a phrase search
            // yet in the full-text search.
            if (funcExpr.getFunctionIdentifier() == BuiltinFunctions.FULLTEXT_CONTAINS
                    && arg2.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                checkFTSearchConstantExpression(constExpression);
            }

            VariableReferenceExpression varExpr = (VariableReferenceExpression) arg1;
            fieldVar = varExpr.getVariableReference();
        } else {
            return false;
        }

        // Updates the given Analysis Context by adding a new optimizable function expression.
        constructNewOptFuncExprAndAddToAnalysisCtx(funcExpr, fieldVar, constExpression, constantExpressionType,
                analysisCtx);
        return true;
    }

    private static void constructNewOptFuncExprAndAddToAnalysisCtx(AbstractFunctionCallExpression funcExpr,
            LogicalVariable fieldVar, ILogicalExpression expression, IAType expressionType,
            AccessMethodAnalysisContext analysisCtx) {
        OptimizableFuncExpr newOptFuncExpr = new OptimizableFuncExpr(funcExpr, fieldVar, expression, expressionType);
        addNewOptFuncExprToAnalysisCtx(funcExpr, newOptFuncExpr, analysisCtx);
    }

    private static void constructNewOptFuncExprAndAddToAnalysisCtx(AbstractFunctionCallExpression funcExpr,
            LogicalVariable[] fieldVars, ILogicalExpression[] expressions, IAType[] expressionTypes,
            AccessMethodAnalysisContext analysisCtx) {
        OptimizableFuncExpr newOptFuncExpr = new OptimizableFuncExpr(funcExpr, fieldVars, expressions, expressionTypes);
        addNewOptFuncExprToAnalysisCtx(funcExpr, newOptFuncExpr, analysisCtx);

    }

    private static void addNewOptFuncExprToAnalysisCtx(AbstractFunctionCallExpression funcExpr,
            OptimizableFuncExpr newOptFuncExpr, AccessMethodAnalysisContext analysisCtx) {
        for (IOptimizableFuncExpr optFuncExpr : analysisCtx.getMatchedFuncExprs()) {
            //avoid additional optFuncExpressions in case of a join
            if (optFuncExpr.getFuncExpr().equals(funcExpr)) {
                return;
            }
        }
        analysisCtx.addMatchedFuncExpr(newOptFuncExpr);
    }

    /**
     * Fetches each element and calls the check for the type and value in the given list using the given cursor.
     */
    private static void checkEachElementInFTSearchListPredicate(IACursor oListCursor) throws AlgebricksException {
        String argValue;
        IAObject element;
        while (oListCursor.next()) {
            element = oListCursor.get();
            if (element.getType() == BuiltinType.ASTRING) {
                argValue = ConstantExpressionUtil.getStringConstant(element);
                checkAndGenerateFTSearchExceptionForStringPhrase(argValue);
            } else {
                throw new CompilationException(ErrorCode.COMPILATION_TYPE_UNSUPPORTED,
                        BuiltinFunctions.FULLTEXT_CONTAINS.getName(), element.getType().getTypeTag());
            }
        }
    }

    // Checks whether a proper constant expression is in place for the full-text search.
    // A proper constant expression in the full-text search should be among string, string type (Un)ordered list.
    public static void checkFTSearchConstantExpression(ILogicalExpression constExpression) throws AlgebricksException {
        IAObject objectFromExpr = ConstantExpressionUtil.getConstantIaObject(constExpression, null);
        String arg2Value;
        IACursor oListCursor;

        switch (objectFromExpr.getType().getTypeTag()) {
            case STRING:
                arg2Value = ConstantExpressionUtil.getStringConstant(objectFromExpr);
                checkAndGenerateFTSearchExceptionForStringPhrase(arg2Value);
                break;
            case ARRAY:
                oListCursor = ConstantExpressionUtil.getOrderedListConstant(objectFromExpr).getCursor();
                checkEachElementInFTSearchListPredicate(oListCursor);
                break;
            case MULTISET:
                oListCursor = ConstantExpressionUtil.getUnorderedListConstant(objectFromExpr).getCursor();
                checkEachElementInFTSearchListPredicate(oListCursor);
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_TYPE_UNSUPPORTED,
                        BuiltinFunctions.FULLTEXT_CONTAINS.getName(), objectFromExpr.getType().getTypeTag());
        }
    }

    // Checks whether the given string is a phrase. If so, generates an exception since
    // we don't support a phrase search in the full-text search yet.
    public static void checkAndGenerateFTSearchExceptionForStringPhrase(String value) throws AlgebricksException {
        for (int j = 0; j < value.length(); j++) {
            if (DelimitedUTF8StringBinaryTokenizer.isSeparator(value.charAt(j))) {
                throw new CompilationException(ErrorCode.COMPILATION_FULLTEXT_PHRASE_FOUND);
            }
        }
    }

    public static boolean analyzeFuncExprArgsForTwoVarsAndUpdateAnalysisCtx(AbstractFunctionCallExpression funcExpr,
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

        // Updates the given Analysis Context by adding a new optimizable function expression.
        constructNewOptFuncExprAndAddToAnalysisCtx(funcExpr, new LogicalVariable[] { fieldVar1, fieldVar2 },
                new ILogicalExpression[0], new IAType[0], analysisCtx);
        return true;
    }

    /**
     * Appends the types of the fields produced by the given secondary index to dest.
     */
    public static void appendSecondaryIndexTypes(Dataset dataset, ARecordType recordType, ARecordType metaRecordType,
            Index index, boolean primaryKeysOnly, List<Object> dest) throws AlgebricksException {
        if (!primaryKeysOnly) {
            switch (index.getIndexType()) {
                case BTREE:
                    dest.addAll(KeyFieldTypeUtil.getBTreeIndexKeyTypes(index, recordType, metaRecordType));
                    break;
                case RTREE:
                    dest.addAll(KeyFieldTypeUtil.getRTreeIndexKeyTypes(index, recordType, metaRecordType));
                    break;
                case SINGLE_PARTITION_WORD_INVIX:
                case SINGLE_PARTITION_NGRAM_INVIX:
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
            dest.addAll(KeyFieldTypeUtil.getPartitoningKeyTypes(dataset, recordType, metaRecordType));
        }
    }

    public static void appendSecondaryIndexOutputVars(Dataset dataset, ARecordType recordType,
            ARecordType metaRecordType, Index index, boolean primaryKeysOnly, IOptimizationContext context,
            List<LogicalVariable> dest) throws AlgebricksException {
        int numPrimaryKeys = 0;
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            numPrimaryKeys = IndexingConstants
                    .getRIDSize(((ExternalDatasetDetails) dataset.getDatasetDetails()).getProperties());
        } else {
            numPrimaryKeys = dataset.getPrimaryKeys().size();
        }
        int numSecondaryKeys = KeyFieldTypeUtil.getNumSecondaryKeys(index, recordType, metaRecordType);
        int numVars = (primaryKeysOnly) ? numPrimaryKeys : numPrimaryKeys + numSecondaryKeys;
        for (int i = 0; i < numVars; i++) {
            dest.add(context.newVar());
        }
    }

    public static List<LogicalVariable> getPrimaryKeyVarsFromSecondaryUnnestMap(Dataset dataset,
            ILogicalOperator unnestMapOp) {
        int numPrimaryKeys;
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            numPrimaryKeys = IndexingConstants
                    .getRIDSize(((ExternalDatasetDetails) dataset.getDatasetDetails()).getProperties());
        } else {
            numPrimaryKeys = dataset.getPrimaryKeys().size();
        }
        List<LogicalVariable> primaryKeyVars = new ArrayList<>();
        List<LogicalVariable> sourceVars = null;

        sourceVars = ((AbstractUnnestMapOperator) unnestMapOp).getVariables();

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
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        List<LogicalVariable> primaryKeyVars = new ArrayList<>();
        List<LogicalVariable> sourceVars = null;

        // For a left outer join case, LEFT_OUTER_UNNEST_MAP operator is placed
        // instead of UNNEST_MAP operator.
        sourceVars = ((AbstractUnnestMapOperator) unnestMapOp).getVariables();

        // Assumes the primary keys are located at the beginning.
        for (int i = 0; i < numPrimaryKeys; i++) {
            primaryKeyVars.add(sourceVars.get(i));
        }
        return primaryKeyVars;
    }

    /**
     * Returns the search key expression which feeds a secondary-index search. If we are optimizing a selection query
     * then this method returns the a ConstantExpression from the first constant value in the optimizable function
     * expression.
     * If we are optimizing a join, then this method returns the VariableReferenceExpression that should feed the
     * secondary index probe.
     *
     * @throws AlgebricksException
     */
    public static Pair<ILogicalExpression, Boolean> createSearchKeyExpr(Index index, IOptimizableFuncExpr optFuncExpr,
            IAType indexedFieldType, OptimizableOperatorSubTree indexSubTree, OptimizableOperatorSubTree probeSubTree)
            throws AlgebricksException {

        if (probeSubTree == null) {
            // We are optimizing a selection query. Search key is a constant.
            // Type Checking and type promotion is done here

            if (optFuncExpr.getNumConstantExpr() == 0) {
                //We are looking at a selection case, but using two variables
                //This means that the second variable comes from a nonPure function call
                //TODO: Right now we miss on type promotion for nonpure functions
                return new Pair<>(new VariableReferenceExpression(optFuncExpr.getLogicalVar(1)), false);
            }

            ILogicalExpression constantAtRuntimeExpression = optFuncExpr.getConstantExpr(0);
            AsterixConstantValue constantValue = null;
            if (constantAtRuntimeExpression.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                constantValue = (AsterixConstantValue) ((ConstantExpression) constantAtRuntimeExpression).getValue();
            }

            ATypeTag constantValueTag = optFuncExpr.getConstantType(0).getTypeTag();
            ATypeTag indexedFieldTypeTag = TypeComputeUtils.getActualType(indexedFieldType).getTypeTag();

            // if the constant type and target type does not match, we do a type conversion
            AsterixConstantValue replacedConstantValue = null;
            // type casting happened from real (FLOAT, DOUBLE) value -> INT value?
            boolean realTypeConvertedToIntegerType = false;

            if (constantValueTag != indexedFieldTypeTag && constantValue != null) {
                try {
                    replacedConstantValue = ATypeHierarchy.getAsterixConstantValueFromNumericTypeObject(
                            constantValue.getObject(), indexedFieldTypeTag, index.isEnforced());
                    realTypeConvertedToIntegerType =
                            isRealTypeConvertedToIntegerType(constantValueTag, indexedFieldTypeTag);
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
            }

            return replacedConstantValue != null
                    ? new Pair<>(new ConstantExpression(replacedConstantValue), realTypeConvertedToIntegerType)
                    : new Pair<>(constantAtRuntimeExpression, false);
        } else {
            // We are optimizing a join query. Determine which variable feeds the secondary index.
            OptimizableOperatorSubTree opSubTree0 = optFuncExpr.getOperatorSubTree(0);
            int probeVarIndex = opSubTree0 == null || opSubTree0 == probeSubTree ? 0 : 1;
            LogicalVariable probeVar = optFuncExpr.getLogicalVar(probeVarIndex);
            ILogicalExpression probeExpr = new VariableReferenceExpression(probeVar);

            ATypeTag indexedFieldTypeTag = TypeComputeUtils.getActualType(indexedFieldType).getTypeTag();
            if (ATypeHierarchy.getTypeDomain(indexedFieldTypeTag) == ATypeHierarchy.Domain.NUMERIC) {
                IAType probeType = TypeComputeUtils.getActualType(optFuncExpr.getFieldType(probeVarIndex));
                ATypeTag probeTypeTypeTag = probeType.getTypeTag();
                if (probeTypeTypeTag != indexedFieldTypeTag) {
                    ScalarFunctionCallExpression castFunc = new ScalarFunctionCallExpression(
                            FunctionUtil.getFunctionInfo(BuiltinFunctions.CAST_TYPE_LAX));
                    castFunc.getArguments().add(new MutableObject<>(probeExpr));
                    TypeCastUtils.setRequiredAndInputTypes(castFunc, indexedFieldType, probeType);
                    boolean realTypeConvertedToIntegerType =
                            isRealTypeConvertedToIntegerType(probeTypeTypeTag, indexedFieldTypeTag);
                    return new Pair<>(castFunc, realTypeConvertedToIntegerType);
                }
            }

            return new Pair<>(probeExpr, false);
        }
    }

    private static boolean isRealTypeConvertedToIntegerType(ATypeTag probeTypeTag, ATypeTag indexedFieldTypeTag) {
        // To check whether the constant is REAL values, and target field is an INT type field.
        // In this case, we need to change the search parameter. Refer to the caller section for the detail.
        switch (probeTypeTag) {
            case DOUBLE:
            case FLOAT:
                switch (indexedFieldTypeTag) {
                    case TINYINT:
                    case SMALLINT:
                    case INTEGER:
                    case BIGINT:
                        return true;
                    default:
                        break;
                }
                break;
            default:
                break;
        }
        return false;
    }

    /**
     * Returns the first expr optimizable by this index.
     */
    public static IOptimizableFuncExpr chooseFirstOptFuncExpr(Index chosenIndex,
            AccessMethodAnalysisContext analysisCtx) {
        List<Pair<Integer, Integer>> indexExprs = analysisCtx.getIndexExprsFromIndexExprsAndVars(chosenIndex);
        int firstExprIndex = indexExprs.get(0).first;
        return analysisCtx.getMatchedFuncExpr(firstExprIndex);
    }

    public static int chooseFirstOptFuncVar(Index chosenIndex, AccessMethodAnalysisContext analysisCtx) {
        List<Pair<Integer, Integer>> indexExprs = analysisCtx.getIndexExprsFromIndexExprsAndVars(chosenIndex);
        return indexExprs.get(0).second;
    }

    public static ILogicalOperator createSecondaryIndexUnnestMap(Dataset dataset, ARecordType recordType,
            ARecordType metaRecordType, Index index, ILogicalOperator inputOp, AccessMethodJobGenParams jobGenParams,
            IOptimizationContext context, boolean outputPrimaryKeysOnly, boolean retainInput, boolean retainNull)
            throws AlgebricksException {
        // The job gen parameters are transferred to the actual job gen via the UnnestMapOperator's function arguments.
        ArrayList<Mutable<ILogicalExpression>> secondaryIndexFuncArgs = new ArrayList<>();
        jobGenParams.writeToFuncArgs(secondaryIndexFuncArgs);
        // Variables and types coming out of the secondary-index search.
        List<LogicalVariable> secondaryIndexUnnestVars = new ArrayList<>();
        List<Object> secondaryIndexOutputTypes = new ArrayList<>();
        // Append output variables/types generated by the secondary-index search (not forwarded from input).
        appendSecondaryIndexOutputVars(dataset, recordType, metaRecordType, index, outputPrimaryKeysOnly, context,
                secondaryIndexUnnestVars);
        appendSecondaryIndexTypes(dataset, recordType, metaRecordType, index, outputPrimaryKeysOnly,
                secondaryIndexOutputTypes);
        // An index search is expressed as an unnest over an index-search function.
        IFunctionInfo secondaryIndexSearch = FunctionUtil.getFunctionInfo(BuiltinFunctions.INDEX_SEARCH);
        UnnestingFunctionCallExpression secondaryIndexSearchFunc =
                new UnnestingFunctionCallExpression(secondaryIndexSearch, secondaryIndexFuncArgs);
        secondaryIndexSearchFunc.setReturnsUniqueValues(true);
        // This is the operator that jobgen will be looking for. It contains an unnest function that has all necessary arguments to determine
        // which index to use, which variables contain the index-search keys, what is the original dataset, etc.

        // Left-outer-join (retainInput and retainNull) case?
        // Then, we use the LEFT-OUTER-UNNEST-MAP operator instead of unnest-map operator.
        if (retainNull) {
            if (retainInput) {
                LeftOuterUnnestMapOperator secondaryIndexLeftOuterUnnestOp = new LeftOuterUnnestMapOperator(
                        secondaryIndexUnnestVars, new MutableObject<ILogicalExpression>(secondaryIndexSearchFunc),
                        secondaryIndexOutputTypes, true);
                secondaryIndexLeftOuterUnnestOp.getInputs().add(new MutableObject<>(inputOp));
                context.computeAndSetTypeEnvironmentForOperator(secondaryIndexLeftOuterUnnestOp);
                secondaryIndexLeftOuterUnnestOp.setExecutionMode(ExecutionMode.PARTITIONED);
                return secondaryIndexLeftOuterUnnestOp;
            } else {
                // Left-outer-join without retainInput doesn't make sense.
                throw new AlgebricksException("Left-outer-join should propagate all inputs from the outer branch.");
            }
        } else {
            // If this is not a left-outer-join case, then we use UNNEST-MAP operator.
            UnnestMapOperator secondaryIndexUnnestOp = new UnnestMapOperator(secondaryIndexUnnestVars,
                    new MutableObject<ILogicalExpression>(secondaryIndexSearchFunc), secondaryIndexOutputTypes,
                    retainInput);
            secondaryIndexUnnestOp.getInputs().add(new MutableObject<>(inputOp));
            context.computeAndSetTypeEnvironmentForOperator(secondaryIndexUnnestOp);
            secondaryIndexUnnestOp.setExecutionMode(ExecutionMode.PARTITIONED);
            return secondaryIndexUnnestOp;
        }
    }

    public static AbstractUnnestMapOperator createPrimaryIndexUnnestMap(AbstractDataSourceOperator dataSourceOp,
            Dataset dataset, ARecordType recordType, ARecordType metaRecordType, ILogicalOperator inputOp,
            IOptimizationContext context, boolean sortPrimaryKeys, boolean retainInput, boolean retainNull,
            boolean requiresBroadcast) throws AlgebricksException {
        List<LogicalVariable> primaryKeyVars =
                AccessMethodUtils.getPrimaryKeyVarsFromSecondaryUnnestMap(dataset, inputOp);
        // Optionally add a sort on the primary-index keys before searching the primary index.
        OrderOperator order = null;
        if (sortPrimaryKeys) {
            order = new OrderOperator();
            for (LogicalVariable pkVar : primaryKeyVars) {
                Mutable<ILogicalExpression> vRef = new MutableObject<>(new VariableReferenceExpression(pkVar));
                order.getOrderExpressions().add(new Pair<>(OrderOperator.ASC_ORDER, vRef));
            }
            // The secondary-index search feeds into the sort.
            order.getInputs().add(new MutableObject<>(inputOp));
            order.setExecutionMode(ExecutionMode.LOCAL);
            context.computeAndSetTypeEnvironmentForOperator(order);
        }
        // The job gen parameters are transferred to the actual job gen via the UnnestMapOperator's function arguments.
        List<Mutable<ILogicalExpression>> primaryIndexFuncArgs = new ArrayList<>();
        BTreeJobGenParams jobGenParams = new BTreeJobGenParams(dataset.getDatasetName(), IndexType.BTREE,
                dataset.getDataverseName(), dataset.getDatasetName(), retainInput, requiresBroadcast);
        // Set low/high inclusive to true for a point lookup.
        jobGenParams.setLowKeyInclusive(true);
        jobGenParams.setHighKeyInclusive(true);
        jobGenParams.setLowKeyVarList(primaryKeyVars, 0, primaryKeyVars.size());
        jobGenParams.setHighKeyVarList(primaryKeyVars, 0, primaryKeyVars.size());
        jobGenParams.setIsEqCondition(true);
        jobGenParams.writeToFuncArgs(primaryIndexFuncArgs);
        // Variables and types coming out of the primary-index search.
        List<LogicalVariable> primaryIndexUnnestVars = new ArrayList<>();
        List<Object> primaryIndexOutputTypes = new ArrayList<>();
        // Append output variables/types generated by the primary-index search (not forwarded from input).
        primaryIndexUnnestVars.addAll(dataSourceOp.getVariables());
        appendPrimaryIndexTypes(dataset, recordType, metaRecordType, primaryIndexOutputTypes);
        // An index search is expressed as an unnest over an index-search function.
        IFunctionInfo primaryIndexSearch = FunctionUtil.getFunctionInfo(BuiltinFunctions.INDEX_SEARCH);
        AbstractFunctionCallExpression primaryIndexSearchFunc =
                new ScalarFunctionCallExpression(primaryIndexSearch, primaryIndexFuncArgs);
        // This is the operator that jobgen will be looking for. It contains an unnest function that has all necessary arguments to determine
        // which index to use, which variables contain the index-search keys, what is the original dataset, etc.
        AbstractUnnestMapOperator primaryIndexUnnestOp = null;
        if (retainNull) {
            if (retainInput) {
                primaryIndexUnnestOp = new LeftOuterUnnestMapOperator(primaryIndexUnnestVars,
                        new MutableObject<ILogicalExpression>(primaryIndexSearchFunc), primaryIndexOutputTypes,
                        retainInput);
            } else {
                // Left-outer-join without retainNull and retainInput doesn't make sense.
                throw new AlgebricksException("Left-outer-join should propagate all inputs from the outer branch.");
            }
        } else {
            primaryIndexUnnestOp = new UnnestMapOperator(primaryIndexUnnestVars,
                    new MutableObject<ILogicalExpression>(primaryIndexSearchFunc), primaryIndexOutputTypes,
                    retainInput);
        }
        // Fed by the order operator or the secondaryIndexUnnestOp.
        if (sortPrimaryKeys) {
            primaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(order));
        } else {
            primaryIndexUnnestOp.getInputs().add(new MutableObject<>(inputOp));
        }
        context.computeAndSetTypeEnvironmentForOperator(primaryIndexUnnestOp);
        primaryIndexUnnestOp.setExecutionMode(ExecutionMode.PARTITIONED);
        return primaryIndexUnnestOp;
    }

    public static ScalarFunctionCallExpression findLOJIsMissingFuncInGroupBy(GroupByOperator lojGroupbyOp)
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
                        ScalarFunctionCallExpression notFuncExpr =
                                (ScalarFunctionCallExpression) selectOp.getCondition().getValue();
                        if (notFuncExpr.getArguments().get(0).getValue()
                                .getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                            if (((AbstractFunctionCallExpression) notFuncExpr.getArguments().get(0).getValue())
                                    .getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.IS_MISSING)) {
                                isNullFuncExpr =
                                        (ScalarFunctionCallExpression) notFuncExpr.getArguments().get(0).getValue();
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
        int numPrimaryKeys =
                IndexingConstants.getRIDSize(((ExternalDatasetDetails) dataset.getDatasetDetails()).getProperties());
        for (int i = 0; i < numPrimaryKeys; i++) {
            target.add(IndexingConstants.getFieldType(i));
        }
    }

    private static void writeVarList(List<LogicalVariable> varList, List<Mutable<ILogicalExpression>> funcArgs) {
        Mutable<ILogicalExpression> numKeysRef =
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt32(varList.size()))));
        funcArgs.add(numKeysRef);
        for (LogicalVariable keyVar : varList) {
            Mutable<ILogicalExpression> keyVarRef = new MutableObject<>(new VariableReferenceExpression(keyVar));
            funcArgs.add(keyVarRef);
        }
    }

    private static void addStringArg(String argument, List<Mutable<ILogicalExpression>> funcArgs) {
        Mutable<ILogicalExpression> stringRef =
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AString(argument))));
        funcArgs.add(stringRef);
    }

    public static UnnestMapOperator createExternalDataLookupUnnestMap(AbstractDataSourceOperator dataSourceOp,
            Dataset dataset, ARecordType recordType, ILogicalOperator inputOp, IOptimizationContext context,
            boolean retainInput, boolean retainNull) throws AlgebricksException {
        List<LogicalVariable> primaryKeyVars =
                AccessMethodUtils.getPrimaryKeyVarsFromSecondaryUnnestMap(dataset, inputOp);

        // add a sort on the RID fields before fetching external data.
        OrderOperator order = new OrderOperator();
        for (LogicalVariable pkVar : primaryKeyVars) {
            Mutable<ILogicalExpression> vRef = new MutableObject<>(new VariableReferenceExpression(pkVar));
            order.getOrderExpressions().add(new Pair<>(OrderOperator.ASC_ORDER, vRef));
        }
        // The secondary-index search feeds into the sort.
        order.getInputs().add(new MutableObject<>(inputOp));
        order.setExecutionMode(ExecutionMode.LOCAL);
        context.computeAndSetTypeEnvironmentForOperator(order);
        List<Mutable<ILogicalExpression>> externalLookupArgs = new ArrayList<>();
        //Add dataverse to the arguments
        AccessMethodUtils.addStringArg(dataset.getDataverseName(), externalLookupArgs);
        //Add dataset to the arguments
        AccessMethodUtils.addStringArg(dataset.getDatasetName(), externalLookupArgs);
        //Add PK vars to the arguments
        AccessMethodUtils.writeVarList(primaryKeyVars, externalLookupArgs);

        // Variables and types coming out of the external access.
        List<LogicalVariable> externalUnnestVars = new ArrayList<>();
        List<Object> outputTypes = new ArrayList<>();
        // Append output variables/types generated by the data scan (not forwarded from input).
        externalUnnestVars.addAll(dataSourceOp.getVariables());
        appendExternalRecTypes(dataset, recordType, outputTypes);

        IFunctionInfo externalLookup = FunctionUtil.getFunctionInfo(BuiltinFunctions.EXTERNAL_LOOKUP);
        AbstractFunctionCallExpression externalLookupFunc =
                new ScalarFunctionCallExpression(externalLookup, externalLookupArgs);
        UnnestMapOperator unnestOp = new UnnestMapOperator(externalUnnestVars,
                new MutableObject<ILogicalExpression>(externalLookupFunc), outputTypes, retainInput);
        // Fed by the order operator or the secondaryIndexUnnestOp.
        unnestOp.getInputs().add(new MutableObject<ILogicalOperator>(order));

        context.computeAndSetTypeEnvironmentForOperator(unnestOp);
        unnestOp.setExecutionMode(ExecutionMode.PARTITIONED);

        //set the physical operator
        DataSourceId dataSourceId = new DataSourceId(dataset.getDataverseName(), dataset.getDatasetName());
        unnestOp.setPhysicalOperator(new ExternalDataLookupPOperator(dataSourceId, dataset, recordType, primaryKeyVars,
                false, retainInput, retainNull));
        return unnestOp;
    }

    //If the expression is constant at runtime, return the type
    public static IAType constantRuntimeResultType(ILogicalExpression expr, IOptimizationContext context,
            IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {
        Set<LogicalVariable> usedVariables = new HashSet<>();
        expr.getUsedVariables(usedVariables);
        if (usedVariables.size() > 0) {
            return null;
        }
        return (IAType) context.getExpressionTypeComputer().getType(expr, context.getMetadataProvider(),
                typeEnvironment);
    }

    //Get Variables used by afterSelectRefs that were created before the datasource
    //If there are any, we should retain inputs
    public static boolean retainInputs(List<LogicalVariable> dataSourceVariables, ILogicalOperator sourceOp,
            List<Mutable<ILogicalOperator>> afterSelectRefs) throws AlgebricksException {
        List<LogicalVariable> usedVars = new ArrayList<>();
        List<LogicalVariable> producedVars = new ArrayList<>();
        List<LogicalVariable> liveVars = new ArrayList<>();
        VariableUtilities.getLiveVariables(sourceOp, liveVars);
        for (Mutable<ILogicalOperator> opMutable : afterSelectRefs) {
            ILogicalOperator op = opMutable.getValue();
            VariableUtilities.getUsedVariables(op, usedVars);
            VariableUtilities.getProducedVariables(op, producedVars);
        }
        usedVars.removeAll(producedVars);
        usedVars.removeAll(dataSourceVariables);
        usedVars.retainAll(liveVars);
        return usedVars.isEmpty() ? false : true;
    }

}
