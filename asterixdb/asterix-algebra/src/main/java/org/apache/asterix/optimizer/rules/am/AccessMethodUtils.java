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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.algebra.operators.physical.ExternalDataLookupPOperator;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
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
import org.apache.asterix.om.types.hierachy.ATypeHierarchy.TypeCastingMathFunctionType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Quadruple;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
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
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractDataSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SplitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizer;

/**
 * Static helper functions for rewriting plans using indexes.
 */
public class AccessMethodUtils {

    // Output variable type from a secondary unnest-map
    enum SecondaryUnnestMapOutputVarType {
        PRIMARY_KEY,
        SECONDARY_KEY,
        CONDITIONAL_SPLIT_VAR
    }

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
                        constExpression.getSourceLocation(), BuiltinFunctions.FULLTEXT_CONTAINS.getName(),
                        objectFromExpr.getType().getTypeTag());
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
            Index index, List<Object> dest, boolean requireResultOfInstantTryLock) throws AlgebricksException {
        // In case of an inverted-index search, secondary keys will not be generated.
        boolean primaryKeysOnly = isInvertedIndex(index);
        if (!primaryKeysOnly) {
            switch (index.getIndexType()) {
                case BTREE:
                    dest.addAll(KeyFieldTypeUtil.getBTreeIndexKeyTypes(index, recordType, metaRecordType));
                    break;
                case RTREE:
                    dest.addAll(KeyFieldTypeUtil.getRTreeIndexKeyTypes(index, recordType, metaRecordType));
                    break;
                default:
                    break;
            }
        }
        // Primary keys.
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            //add primary keys
            appendExternalRecPrimaryKeys(dataset, dest);
        } else {
            dest.addAll(KeyFieldTypeUtil.getPartitoningKeyTypes(dataset, recordType, metaRecordType));
        }

        // Adds one more type to apply an index-only plan optimization.
        // Currently, we use AINT32 to decode result values for this.
        // Refer to appendSecondaryIndexOutputVars() for more details.
        if (requireResultOfInstantTryLock) {
            dest.add(BuiltinType.AINT32);
        }

    }

    /**
     * Creates output variables for the given unnest-map or left-outer-unnestmap operator
     * that does a secondary index lookup.
     * The order: SK, PK, [Optional: the result of a instantTryLock on PK]
     */
    public static void appendSecondaryIndexOutputVars(Dataset dataset, ARecordType recordType,
            ARecordType metaRecordType, Index index, IOptimizationContext context, List<LogicalVariable> dest,
            boolean requireResultOfInstantTryLock) throws AlgebricksException {
        int numPrimaryKeys;
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            numPrimaryKeys = IndexingConstants
                    .getRIDSize(((ExternalDatasetDetails) dataset.getDatasetDetails()).getProperties());
        } else {
            numPrimaryKeys = dataset.getPrimaryKeys().size();
        }
        int numSecondaryKeys = KeyFieldTypeUtil.getNumSecondaryKeys(index, recordType, metaRecordType);
        // In case of an inverted-index search, secondary keys will not be generated.
        int numVars = isInvertedIndex(index) ? numPrimaryKeys : numPrimaryKeys + numSecondaryKeys;

        // If it's an index-only plan, add one more variable to put the result of instantTryLock on PK -
        // whether this lock can be granted on a primary key.
        // If it is granted, then we don't need to do a post verification (select).
        // If it is not granted, then we need to do a secondary index lookup, do a primary index lookup, and select.
        if (requireResultOfInstantTryLock) {
            numVars += 1;
        }

        for (int i = 0; i < numVars; i++) {
            dest.add(context.newVar());
        }
    }

    /**
     * Gets the primary key variables from the unnest-map or left-outer-unnest-map operator
     * that does a secondary index lookup.
     * The order: SK, PK, [Optional: the result of a TryLock on PK]
     */
    public static List<LogicalVariable> getKeyVarsFromSecondaryUnnestMap(Dataset dataset, ARecordType recordType,
            ARecordType metaRecordType, ILogicalOperator unnestMapOp, Index index,
            SecondaryUnnestMapOutputVarType keyVarType) throws AlgebricksException {
        int numPrimaryKeys;
        int numSecondaryKeys = KeyFieldTypeUtil.getNumSecondaryKeys(index, recordType, metaRecordType);
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            numPrimaryKeys = IndexingConstants
                    .getRIDSize(((ExternalDatasetDetails) dataset.getDatasetDetails()).getProperties());
        } else {
            numPrimaryKeys = dataset.getPrimaryKeys().size();
        }
        List<LogicalVariable> keyVars = new ArrayList<>();
        AbstractUnnestMapOperator abstractUnnestMapOp = (AbstractUnnestMapOperator) unnestMapOp;
        List<LogicalVariable> sourceVars = abstractUnnestMapOp.getVariables();
        // Assumption: the primary keys are located after the secondary key.
        int start;
        int stop;

        // If a secondary-index search didn't generate SKs, set it to zero.
        // Currently, only an inverted-index search doesn't generate any SKs.
        boolean isNgramOrKeywordIndex = isInvertedIndex(index);
        if (isNgramOrKeywordIndex) {
            numSecondaryKeys = 0;
        }

        // Fetches keys: type 0 - PK, type 1 - SK, type 2 - the result of instantTryLock() on PK
        switch (keyVarType) {
            case PRIMARY_KEY:
                // Fetches primary keys - the second position
                start = numSecondaryKeys;
                stop = numSecondaryKeys + numPrimaryKeys;
                break;
            case SECONDARY_KEY:
                // Fetches secondary keys - the first position
                start = 0;
                stop = numSecondaryKeys;
                break;
            case CONDITIONAL_SPLIT_VAR:
                // Sanity check - the given unnest map should generate this variable.
                if (!abstractUnnestMapOp.getGenerateCallBackProceedResultVar()) {
                    throw CompilationException.create(ErrorCode.CANNOT_GET_CONDITIONAL_SPLIT_KEY_VARIABLE,
                            unnestMapOp.getSourceLocation());
                }
                // Fetches conditional splitter - the last position
                start = numSecondaryKeys + numPrimaryKeys;
                stop = start + 1;
                break;
            default:
                return Collections.emptyList();
        }
        for (int i = start; i < stop; i++) {
            keyVars.add(sourceVars.get(i));
        }
        return keyVars;
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
    public static Triple<ILogicalExpression, ILogicalExpression, Boolean> createSearchKeyExpr(Index index,
            IOptimizableFuncExpr optFuncExpr, IAType indexedFieldType, OptimizableOperatorSubTree probeSubTree)
            throws AlgebricksException {
        SourceLocation sourceLoc = optFuncExpr.getFuncExpr().getSourceLocation();
        if (probeSubTree == null) {
            // We are optimizing a selection query. Search key is a constant.
            // Type Checking and type promotion is done here

            if (optFuncExpr.getNumConstantExpr() == 0) {
                //We are looking at a selection case, but using two variables
                //This means that the second variable comes from a nonPure function call
                //TODO: Right now we miss on type promotion for nonpure functions
                VariableReferenceExpression varRef = new VariableReferenceExpression(optFuncExpr.getLogicalVar(1));
                varRef.setSourceLocation(sourceLoc);
                return new Triple<>(varRef, null, false);
            }

            ILogicalExpression constantAtRuntimeExpression = optFuncExpr.getConstantExpr(0);
            AsterixConstantValue constantValue = null;
            if (constantAtRuntimeExpression.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                constantValue = (AsterixConstantValue) ((ConstantExpression) constantAtRuntimeExpression).getValue();
            }

            ATypeTag constantValueTag = optFuncExpr.getConstantType(0).getTypeTag();
            ATypeTag indexedFieldTypeTag = TypeComputeUtils.getActualType(indexedFieldType).getTypeTag();

            // type casting happened from real (FLOAT, DOUBLE) value -> INT value?
            boolean realTypeConvertedToIntegerType = false;
            // constant value after type-casting is applied.
            AsterixConstantValue replacedConstantValue = null;
            // constant value after type-casting is applied - this value is used only for equality case.
            // Refer to the following switch case for more details.
            AsterixConstantValue replacedConstantValueForEQCase = null;

            // If the constant type and target type does not match, we may need to do a type conversion.
            if (constantValueTag != indexedFieldTypeTag && constantValue != null) {
                // To check whether the constant is REAL values, and target field is an INT type field.
                // In this case, we need to change the search parameter. Refer to the caller section for the detail.
                realTypeConvertedToIntegerType =
                        isRealTypeConvertedToIntegerType(constantValueTag, indexedFieldTypeTag);
                if (realTypeConvertedToIntegerType) {
                    // if a DOUBLE or FLOAT constant is converted to an INT type value,
                    // we need to check a corner case where two real values are located
                    // between an INT value. For example, the following query,
                    //
                    // for $emp in dataset empDataset
                    // where $emp.age > double("2.3") and $emp.age < double("3.3")
                    // return $emp.id;
                    //
                    // It should generate a result if there is a tuple that satisfies the condition,
                    // which is 3. However, it does not generate the desired result since finding
                    // candidates fails after truncating the fraction part
                    // (there is no INT whose value is greater than 2 and less than 3.)
                    //
                    // Thus,
                    // when converting FLOAT or DOUBLE values, we need to apply ceil() or floor().
                    // The following transformation will generate the final result, not a superset of it.
                    //
                    // LT
                    // IntVar < 4.9 ==> round-up: IntVar < 5
                    //
                    // LE
                    // IntVar <= 4.9  ==> round-down: IntVar <= 4
                    //
                    // GT
                    // IntVar > 4.9 ==> round-down: IntVar > 4
                    //
                    // GE
                    // IntVar >= 4.9 ==> round-up: IntVar >= 5
                    //
                    // EQ: two values are required to do a correct type-casting.
                    // IntVar = 4.3 ==> round-down and round-up: IntVar = 4 and IntVar = 5 : false
                    // IntVar = 4.0 ==> round-down and round-up: IntVar = 4 and IntVar = 4 : true
                    FunctionIdentifier functionID = optFuncExpr.getFuncExpr().getFunctionIdentifier();
                    ComparisonKind cKind = AlgebricksBuiltinFunctions.getComparisonType(functionID);

                    switch (cKind) {
                        case LT:
                        case GE:
                            // round-up
                            replacedConstantValue = getReplacedConstantValue(constantValue.getObject(),
                                    constantValueTag, indexedFieldTypeTag, index.isEnforced(),
                                    TypeCastingMathFunctionType.CEIL, sourceLoc);
                            break;
                        case LE:
                        case GT:
                            // round-down
                            replacedConstantValue = getReplacedConstantValue(constantValue.getObject(),
                                    constantValueTag, indexedFieldTypeTag, index.isEnforced(),
                                    TypeCastingMathFunctionType.FLOOR, sourceLoc);
                            break;
                        case EQ:
                            // equality case - both CEIL and FLOOR need to be applied.
                            replacedConstantValue = getReplacedConstantValue(constantValue.getObject(),
                                    constantValueTag, indexedFieldTypeTag, index.isEnforced(),
                                    TypeCastingMathFunctionType.FLOOR, sourceLoc);
                            replacedConstantValueForEQCase = getReplacedConstantValue(constantValue.getObject(),
                                    constantValueTag, indexedFieldTypeTag, index.isEnforced(),
                                    TypeCastingMathFunctionType.CEIL, sourceLoc);
                            break;
                        default:
                            // NEQ should not be a case.
                            throw new IllegalStateException();
                    }
                } else {
                    // Type conversion only case: (e.g., INT -> BIGINT)
                    replacedConstantValue = getReplacedConstantValue(constantValue.getObject(), constantValueTag,
                            indexedFieldTypeTag, index.isEnforced(), TypeCastingMathFunctionType.NONE, sourceLoc);
                }
            }
            // No type-casting at all
            if (replacedConstantValue == null) {
                return new Triple<>(constantAtRuntimeExpression, null, false);
            }
            // A type-casting happened, but not EQ case
            if (replacedConstantValueForEQCase == null) {
                return new Triple<>(new ConstantExpression(replacedConstantValue), null,
                        realTypeConvertedToIntegerType);
            }
            // A type-casting happened and it's an EQ case.
            return new Triple<>(new ConstantExpression(replacedConstantValue),
                    new ConstantExpression(replacedConstantValueForEQCase), realTypeConvertedToIntegerType);
        } else {
            // We are optimizing a join query. Determine which variable feeds the secondary index.
            OptimizableOperatorSubTree opSubTree0 = optFuncExpr.getOperatorSubTree(0);
            int probeVarIndex = opSubTree0 == null || opSubTree0 == probeSubTree ? 0 : 1;
            LogicalVariable probeVar = optFuncExpr.getLogicalVar(probeVarIndex);
            VariableReferenceExpression probeExpr = new VariableReferenceExpression(probeVar);
            probeExpr.setSourceLocation(sourceLoc);

            ATypeTag indexedFieldTypeTag = TypeComputeUtils.getActualType(indexedFieldType).getTypeTag();
            if (ATypeHierarchy.getTypeDomain(indexedFieldTypeTag) == ATypeHierarchy.Domain.NUMERIC) {
                IAType probeType = TypeComputeUtils.getActualType(optFuncExpr.getFieldType(probeVarIndex));
                ATypeTag probeTypeTypeTag = probeType.getTypeTag();
                if (probeTypeTypeTag != indexedFieldTypeTag) {
                    ScalarFunctionCallExpression castFunc = new ScalarFunctionCallExpression(
                            FunctionUtil.getFunctionInfo(BuiltinFunctions.CAST_TYPE_LAX));
                    castFunc.setSourceLocation(sourceLoc);
                    castFunc.getArguments().add(new MutableObject<>(probeExpr));
                    TypeCastUtils.setRequiredAndInputTypes(castFunc, indexedFieldType, probeType);
                    boolean realTypeConvertedToIntegerType =
                            isRealTypeConvertedToIntegerType(probeTypeTypeTag, indexedFieldTypeTag);
                    return new Triple<>(castFunc, null, realTypeConvertedToIntegerType);
                }
            }
            return new Triple<>(probeExpr, null, false);
        }
    }

    private static AsterixConstantValue getReplacedConstantValue(IAObject sourceObject, ATypeTag sourceTypeTag,
            ATypeTag targetTypeTag, boolean strictDemote, TypeCastingMathFunctionType mathFunction,
            SourceLocation sourceLoc) throws CompilationException {
        try {
            return ATypeHierarchy.getAsterixConstantValueFromNumericTypeObject(sourceObject, targetTypeTag,
                    strictDemote, mathFunction);
        } catch (HyracksDataException e) {
            throw new CompilationException(ErrorCode.ERROR_OCCURRED_BETWEEN_TWO_TYPES_CONVERSION, e, sourceLoc,
                    sourceTypeTag, targetTypeTag);
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

    /**
     * Checks whether the given function expression can utilize the given index.
     * If so, checks the given join plan is an index-only plan or not.
     */
    public static boolean setIndexOnlyPlanInfo(List<Mutable<ILogicalOperator>> afterJoinRefs,
            Mutable<ILogicalOperator> joinRef, OptimizableOperatorSubTree probeSubTree,
            OptimizableOperatorSubTree indexSubTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, AbstractFunctionCallExpression funcExpr,
            List<Pair<FunctionIdentifier, Boolean>> funcIdentifiers) throws AlgebricksException {
        // index-only plan possible?
        boolean isIndexOnlyPlan = false;

        // Whether a verification (especially for R-Tree case) is required after the secondary index search
        // In other words, can the chosen method generate any false positive results?
        // Currently, for the B+ Tree index, there cannot be any false positive results except the composite index case.
        boolean requireVerificationAfterSIdxSearch = false;

        // Does the given index can cover all search predicates?
        boolean doesSIdxSearchCoverAllPredicates = false;

        Pair<Boolean, Boolean> functionFalsePositiveCheck =
                AccessMethodUtils.canFunctionGenerateFalsePositiveResultsUsingIndex(funcExpr, funcIdentifiers);

        if (functionFalsePositiveCheck.first) {
            requireVerificationAfterSIdxSearch = functionFalsePositiveCheck.second;
        } else {
            // Function not found?
            return false;
        }

        Quadruple<Boolean, Boolean, Boolean, Boolean> indexOnlyPlanInfo = new Quadruple<>(isIndexOnlyPlan, false,
                requireVerificationAfterSIdxSearch, doesSIdxSearchCoverAllPredicates);

        if (analysisCtx.getIndexDatasetMap().get(chosenIndex).getDatasetType() == DatasetType.INTERNAL) {
            AccessMethodUtils.indexOnlyPlanCheck(afterJoinRefs, joinRef, indexSubTree, probeSubTree, chosenIndex,
                    analysisCtx, context, indexOnlyPlanInfo);
        } else {
            // We don't consider an index on an external dataset to be an index-only plan.
            isIndexOnlyPlan = false;
            indexOnlyPlanInfo.setFirst(isIndexOnlyPlan);
        }

        analysisCtx.setIndexOnlyPlanInfo(indexOnlyPlanInfo);

        return true;
    }

    /**
     * Finalizes the index-nested-loop join plan transformation.
     */
    public static boolean finalizeJoinPlanTransformation(List<Mutable<ILogicalOperator>> afterJoinRefs,
            Mutable<ILogicalOperator> joinRef, OptimizableOperatorSubTree indexSubTree,
            AccessMethodAnalysisContext analysisCtx, IOptimizationContext context, boolean isLeftOuterJoin,
            boolean hasGroupBy, ILogicalOperator indexSearchOp, LogicalVariable newNullPlaceHolderVar,
            Mutable<ILogicalExpression> conditionRef, Dataset dataset) throws AlgebricksException {
        ILogicalOperator finalIndexSearchOp = indexSearchOp;
        if (isLeftOuterJoin && hasGroupBy) {
            ScalarFunctionCallExpression lojFuncExprs = analysisCtx.getLOJIsMissingFuncInGroupBy();
            List<LogicalVariable> lojMissingVariables = new ArrayList<>();
            lojFuncExprs.getUsedVariables(lojMissingVariables);
            boolean lojMissingVarExist = false;
            if (!lojMissingVariables.isEmpty()) {
                lojMissingVarExist = true;
            }

            // Resets the missing place holder variable.
            AccessMethodUtils.resetLOJMissingPlaceholderVarInGroupByOp(analysisCtx, newNullPlaceHolderVar, context);

            // For the index-only plan, if newNullPlaceHolderVar is not in the variable map of the union operator
            // or if the variable is removed during the above method, we need to refresh the variable mapping in UNION.
            finalIndexSearchOp = AccessMethodUtils.resetVariableMappingInUnionOpInIndexOnlyPlan(lojMissingVarExist,
                    lojMissingVariables, indexSearchOp, afterJoinRefs, context);
        }

        boolean isIndexOnlyPlan = analysisCtx.getIndexOnlyPlanInfo().getFirst();
        // If there are any left conditions, add a new select operator on top.
        indexSubTree.getDataSourceRef().setValue(finalIndexSearchOp);
        if (conditionRef.getValue() != null) {
            // If an index-only plan is possible, the whole plan is now changed.
            // Replaces the current path with the new index-only plan.
            if (isIndexOnlyPlan && dataset.getDatasetType() == DatasetType.INTERNAL) {
                // Gets the revised dataSourceRef operator from the secondary index-search.
                ILogicalOperator dataSourceRefOp =
                        AccessMethodUtils.findDataSourceFromIndexUtilizationPlan(finalIndexSearchOp);
                if (dataSourceRefOp != null && (dataSourceRefOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP
                        || dataSourceRefOp.getOperatorTag() == LogicalOperatorTag.LEFT_OUTER_UNNEST_MAP)) {
                    indexSubTree.getDataSourceRef().setValue(dataSourceRefOp);
                }
                // Replaces the current operator with the newly created UNIONALL operator.
                joinRef.setValue(finalIndexSearchOp);
            } else {
                // Non-index only plan case
                indexSubTree.getDataSourceRef().setValue(finalIndexSearchOp);
                SelectOperator topSelectOp = new SelectOperator(conditionRef, isLeftOuterJoin, newNullPlaceHolderVar);
                topSelectOp.setSourceLocation(finalIndexSearchOp.getSourceLocation());
                topSelectOp.getInputs().add(indexSubTree.getRootRef());
                topSelectOp.setExecutionMode(ExecutionMode.LOCAL);
                context.computeAndSetTypeEnvironmentForOperator(topSelectOp);
                joinRef.setValue(topSelectOp);
            }
        } else {
            if (finalIndexSearchOp.getOperatorTag() == LogicalOperatorTag.UNIONALL) {
                joinRef.setValue(finalIndexSearchOp);
            } else {
                joinRef.setValue(indexSubTree.getRootRef().getValue());
            }
        }
        return true;
    }

    public static ILogicalOperator createSecondaryIndexUnnestMap(Dataset dataset, ARecordType recordType,
            ARecordType metaRecordType, Index index, ILogicalOperator inputOp, AccessMethodJobGenParams jobGenParams,
            IOptimizationContext context, boolean retainInput, boolean retainNull,
            boolean generateInstantTrylockResultFromIndexSearch) throws AlgebricksException {
        SourceLocation sourceLoc = inputOp.getSourceLocation();
        // The job gen parameters are transferred to the actual job gen via the UnnestMapOperator's function arguments.
        ArrayList<Mutable<ILogicalExpression>> secondaryIndexFuncArgs = new ArrayList<>();
        jobGenParams.writeToFuncArgs(secondaryIndexFuncArgs);
        // Variables and types coming out of the secondary-index search.
        List<LogicalVariable> secondaryIndexUnnestVars = new ArrayList<>();
        List<Object> secondaryIndexOutputTypes = new ArrayList<>();
        // Append output variables/types generated by the secondary-index search (not forwarded from input).
        // Output: SK, PK, [Optional: the result of instantTryLock]
        appendSecondaryIndexOutputVars(dataset, recordType, metaRecordType, index, context, secondaryIndexUnnestVars,
                generateInstantTrylockResultFromIndexSearch);
        appendSecondaryIndexTypes(dataset, recordType, metaRecordType, index, secondaryIndexOutputTypes,
                generateInstantTrylockResultFromIndexSearch);
        // An index search is expressed as an unnest over an index-search function.
        IFunctionInfo secondaryIndexSearch = FunctionUtil.getFunctionInfo(BuiltinFunctions.INDEX_SEARCH);
        UnnestingFunctionCallExpression secondaryIndexSearchFunc =
                new UnnestingFunctionCallExpression(secondaryIndexSearch, secondaryIndexFuncArgs);
        secondaryIndexSearchFunc.setSourceLocation(sourceLoc);
        secondaryIndexSearchFunc.setReturnsUniqueValues(true);
        // This is the operator that jobgen will be looking for. It contains an unnest function that has all
        // necessary arguments to determine which index to use, which variables contain the index-search keys,
        // what is the original dataset, etc.

        // Left-outer-join (retainInput and retainNull) case?
        // Then, we use the LEFT-OUTER-UNNEST-MAP operator instead of unnest-map operator.
        if (retainNull) {
            if (retainInput) {
                LeftOuterUnnestMapOperator secondaryIndexLeftOuterUnnestOp = new LeftOuterUnnestMapOperator(
                        secondaryIndexUnnestVars, new MutableObject<ILogicalExpression>(secondaryIndexSearchFunc),
                        secondaryIndexOutputTypes, true);
                secondaryIndexLeftOuterUnnestOp.setSourceLocation(sourceLoc);
                secondaryIndexLeftOuterUnnestOp
                        .setGenerateCallBackProceedResultVar(generateInstantTrylockResultFromIndexSearch);
                secondaryIndexLeftOuterUnnestOp.getInputs().add(new MutableObject<>(inputOp));
                context.computeAndSetTypeEnvironmentForOperator(secondaryIndexLeftOuterUnnestOp);
                secondaryIndexLeftOuterUnnestOp.setExecutionMode(ExecutionMode.PARTITIONED);
                return secondaryIndexLeftOuterUnnestOp;
            } else {
                // Left-outer-join without retainInput doesn't make sense.
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Left-outer-join should propagate all inputs from the outer branch.");
            }
        } else {
            // If this is not a left-outer-join case, then we use UNNEST-MAP operator.
            UnnestMapOperator secondaryIndexUnnestOp = new UnnestMapOperator(secondaryIndexUnnestVars,
                    new MutableObject<ILogicalExpression>(secondaryIndexSearchFunc), secondaryIndexOutputTypes,
                    retainInput);
            secondaryIndexUnnestOp.setSourceLocation(sourceLoc);
            secondaryIndexUnnestOp.setGenerateCallBackProceedResultVar(generateInstantTrylockResultFromIndexSearch);
            secondaryIndexUnnestOp.getInputs().add(new MutableObject<>(inputOp));
            context.computeAndSetTypeEnvironmentForOperator(secondaryIndexUnnestOp);
            secondaryIndexUnnestOp.setExecutionMode(ExecutionMode.PARTITIONED);
            return secondaryIndexUnnestOp;
        }
    }

    private static AbstractUnnestMapOperator createFinalNonIndexOnlySearchPlan(Dataset dataset,
            ILogicalOperator inputOp, IOptimizationContext context, boolean sortPrimaryKeys, boolean retainInput,
            boolean retainMissing, boolean requiresBroadcast, List<LogicalVariable> primaryKeyVars,
            List<LogicalVariable> primaryIndexUnnestVars, List<Object> primaryIndexOutputTypes)
            throws AlgebricksException {
        SourceLocation sourceLoc = inputOp.getSourceLocation();
        // Optionally add a sort on the primary-index keys before searching the primary index.
        OrderOperator order = null;
        if (sortPrimaryKeys) {
            order = new OrderOperator();
            order.setSourceLocation(sourceLoc);
            for (LogicalVariable pkVar : primaryKeyVars) {
                VariableReferenceExpression pkVarRef = new VariableReferenceExpression(pkVar);
                pkVarRef.setSourceLocation(sourceLoc);
                Mutable<ILogicalExpression> vRef = new MutableObject<>(pkVarRef);
                order.getOrderExpressions().add(new Pair<>(OrderOperator.ASC_ORDER, vRef));
            }
            // The secondary-index search feeds into the sort.
            order.getInputs().add(new MutableObject<>(inputOp));
            order.setExecutionMode(ExecutionMode.LOCAL);
            context.computeAndSetTypeEnvironmentForOperator(order);
        }

        // Creates the primary-index search unnest-map operator.
        AbstractUnnestMapOperator primaryIndexUnnestMapOp =
                createPrimaryIndexUnnestMapOp(dataset, retainInput, retainMissing, requiresBroadcast, primaryKeyVars,
                        primaryIndexUnnestVars, primaryIndexOutputTypes, sourceLoc);
        if (sortPrimaryKeys) {
            primaryIndexUnnestMapOp.getInputs().add(new MutableObject<ILogicalOperator>(order));
        } else {
            primaryIndexUnnestMapOp.getInputs().add(new MutableObject<>(inputOp));
        }
        context.computeAndSetTypeEnvironmentForOperator(primaryIndexUnnestMapOp);
        primaryIndexUnnestMapOp.setExecutionMode(ExecutionMode.PARTITIONED);
        return primaryIndexUnnestMapOp;
    }

    private static ILogicalOperator createFinalIndexOnlySearchPlan(List<Mutable<ILogicalOperator>> afterTopOpRefs,
            Mutable<ILogicalOperator> topOpRef, Mutable<ILogicalExpression> conditionRef,
            List<Mutable<ILogicalOperator>> assignsBeforeTopOpRef, Dataset dataset, ARecordType recordType,
            ARecordType metaRecordType, ILogicalOperator inputOp, IOptimizationContext context, boolean retainInput,
            boolean retainMissing, boolean requiresBroadcast, Index secondaryIndex,
            AccessMethodAnalysisContext analysisCtx, OptimizableOperatorSubTree subTree,
            LogicalVariable newMissingPlaceHolderForLOJ, List<LogicalVariable> pkVarsFromSIdxUnnestMapOp,
            List<LogicalVariable> primaryIndexUnnestVars, List<Object> primaryIndexOutputTypes)
            throws AlgebricksException {
        SourceLocation sourceLoc = inputOp.getSourceLocation();
        Quadruple<Boolean, Boolean, Boolean, Boolean> indexOnlyPlanInfo = analysisCtx.getIndexOnlyPlanInfo();
        // From now on, we deal with the index-only plan.
        // Initializes the information required for the index-only plan optimization.
        // Fetches SK variable(s) from the secondary-index search operator.
        List<LogicalVariable> skVarsFromSIdxUnnestMap = AccessMethodUtils.getKeyVarsFromSecondaryUnnestMap(dataset,
                recordType, metaRecordType, inputOp, secondaryIndex, SecondaryUnnestMapOutputVarType.SECONDARY_KEY);
        boolean skFieldUsedAfterTopOp = indexOnlyPlanInfo.getSecond();
        boolean requireVerificationAfterSIdxSearch = indexOnlyPlanInfo.getThird();
        ILogicalOperator assignBeforeTopOp;
        UnionAllOperator unionAllOp;
        SelectOperator newSelectOpInLeftPath;
        SelectOperator newSelectOpInRightPath;
        SplitOperator splitOp = null;
        // This variable map will be used as input to UNIONALL operator. The form is <left, right, output>.
        // In our case, left: instantTryLock fail path, right: instantTryLock success path
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> unionVarMap = new ArrayList<>();
        List<LogicalVariable> condSplitVars;
        List<LogicalVariable> liveVarsAfterTopOp = new ArrayList<>();

        // Constructs the variable mapping between newly constructed secondary
        // key search (SK, PK) and those in the original plan (datasource scan).
        LinkedHashMap<LogicalVariable, LogicalVariable> origVarToSIdxUnnestMapOpVarMap = new LinkedHashMap<>();

        List<List<String>> chosenIndexFieldNames = secondaryIndex.getKeyFieldNames();
        IndexType idxType = secondaryIndex.getIndexType();

        // variables used in SELECT or JOIN operator
        List<LogicalVariable> usedVarsInTopOp = new ArrayList<>();
        List<LogicalVariable> uniqueUsedVarsInTopOp = new ArrayList<>();

        // variables used in ASSIGN before SELECT operator
        List<LogicalVariable> producedVarsInAssignsBeforeTopOp = new ArrayList<>();

        // For the index-nested-loop join case, we need to exclude the variables from the left (outer) branch
        // when deciding which variables should be propagated via UNIONALL operator.
        // This is because these variables are already generated and is not related to the decision
        // whether the plan is an index-only plan or not. Only the right (inner) branch matters.
        List<LogicalVariable> liveVarsInSubTreeRootOp = new ArrayList<>();

        // variables used after SELECT or JOIN operator
        List<LogicalVariable> usedVarsAfterTopOp = new ArrayList<>();
        List<LogicalVariable> varsTmpList = new ArrayList<>();

        // If the secondary key field is used after SELECT or JOIN operator (e.g., returning the field value),
        // then we need to keep these secondary keys. In case of R-tree index, the result of an R-tree
        // index search is an MBR. So, we need to reconstruct original field values from the result if that index
        // is on a rectangle or point.
        AssignOperator skVarAssignOpInRightPath = null;
        List<LogicalVariable> restoredSKVarFromRTree = null;
        // Original SK field variable to restored SK field variable in the right path mapping
        LinkedHashMap<LogicalVariable, LogicalVariable> origSKFieldVarToNewSKFieldVarMap = new LinkedHashMap<>();
        // Index-only plan consideration for the R-Tree index only:
        // Constructs an additional ASSIGN to restore the original secondary key field(s) from
        // the results of the secondary index search in case the field is used after SELECT or JOIN operator or
        // a verification is required since the given query shape is not RECTANGLE or POINT even though the type of
        // index is RECTANGLE or POINT (in this case only, removing false-positive is possible.).
        if (idxType == IndexType.RTREE && (skFieldUsedAfterTopOp || requireVerificationAfterSIdxSearch)) {
            IOptimizableFuncExpr optFuncExpr = AccessMethodUtils.chooseFirstOptFuncExpr(secondaryIndex, analysisCtx);
            int optFieldIdx = AccessMethodUtils.chooseFirstOptFuncVar(secondaryIndex, analysisCtx);
            Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(optFuncExpr.getFieldType(optFieldIdx),
                    optFuncExpr.getFieldName(optFieldIdx), recordType);
            if (keyPairType == null) {
                return null;
            }
            // Gets the number of dimensions corresponding to the field indexed by chosenIndex.
            IAType spatialType = keyPairType.first;
            ArrayList<Mutable<ILogicalExpression>> restoredSKFromRTreeExprs = new ArrayList<>();
            restoredSKVarFromRTree = new ArrayList<>();
            switch (spatialType.getTypeTag()) {
                case POINT:
                    // Reconstructs a POINT value.
                    AbstractFunctionCallExpression createPointExpr =
                            createPointExpression(skVarsFromSIdxUnnestMap, sourceLoc);
                    restoredSKVarFromRTree.add(context.newVar());
                    restoredSKFromRTreeExprs.add(new MutableObject<ILogicalExpression>(createPointExpr));
                    skVarAssignOpInRightPath = new AssignOperator(restoredSKVarFromRTree, restoredSKFromRTreeExprs);
                    skVarAssignOpInRightPath.setSourceLocation(sourceLoc);
                    break;
                case RECTANGLE:
                    // Reconstructs a RECTANGLE value.
                    AbstractFunctionCallExpression expr1 =
                            createPointExpression(skVarsFromSIdxUnnestMap.subList(0, 2), sourceLoc);
                    AbstractFunctionCallExpression expr2 =
                            createPointExpression(skVarsFromSIdxUnnestMap.subList(2, 4), sourceLoc);
                    AbstractFunctionCallExpression createRectangleExpr = createRectangleExpression(expr1, expr2);
                    restoredSKVarFromRTree.add(context.newVar());
                    restoredSKFromRTreeExprs.add(new MutableObject<ILogicalExpression>(createRectangleExpr));
                    skVarAssignOpInRightPath = new AssignOperator(restoredSKVarFromRTree, restoredSKFromRTreeExprs);
                    skVarAssignOpInRightPath.setSourceLocation(sourceLoc);
                    break;
                default:
                    break;
            }
        }

        // Gets all variables from the right (inner) branch.
        VariableUtilities.getLiveVariables(subTree.getRootRef().getValue(), liveVarsInSubTreeRootOp);
        // Gets the used variables from the SELECT or JOIN operator.
        VariableUtilities.getUsedVariables(topOpRef.getValue(), usedVarsInTopOp);
        // Excludes the variables in the condition from the outer branch - in join case.
        for (Iterator<LogicalVariable> iterator = usedVarsInTopOp.iterator(); iterator.hasNext();) {
            LogicalVariable v = iterator.next();
            if (!liveVarsInSubTreeRootOp.contains(v)) {
                iterator.remove();
            }
        }
        // Keeps the unique used variables in the SELECT or JOIN operator.
        copyVarsToAnotherList(usedVarsInTopOp, uniqueUsedVarsInTopOp);

        // If there are ASSIGN operators (usually secondary key field) before the given SELECT or JOIN operator,
        // we may need to propagate these produced variables via the UNIONALL operator if they are used afterwards.
        if (assignsBeforeTopOpRef != null && !assignsBeforeTopOpRef.isEmpty()) {
            for (int i = 0; i < assignsBeforeTopOpRef.size(); i++) {
                assignBeforeTopOp = assignsBeforeTopOpRef.get(i).getValue();
                varsTmpList.clear();
                VariableUtilities.getProducedVariables(assignBeforeTopOp, varsTmpList);
                copyVarsToAnotherList(varsTmpList, producedVarsInAssignsBeforeTopOp);
            }
        }

        // Adds an optional ASSIGN operator that sits right after the SELECT or JOIN operator.
        // This assign operator keeps any constant expression(s) extracted from the original ASSIGN operators
        // in the subtree and are used after the SELECT or JOIN operator. In usual case,
        // this constant value would be used in a group-by after a left-outer-join and will be removed by the optimizer.
        // We need to conduct this since this variable does not have to be in the both branch of an index-only plan.
        AssignOperator constAssignOp = null;
        ILogicalOperator currentOpAfterTopOp = null;
        List<LogicalVariable> constAssignVars = new ArrayList<>();
        List<Mutable<ILogicalExpression>> constAssignExprs = new ArrayList<>();
        ILogicalOperator currentOp = inputOp;

        boolean constantAssignVarUsedInTopOp = false;
        if (assignsBeforeTopOpRef != null) {
            // From the first ASSIGN (earliest in the plan) to the last ASSGIN (latest)
            for (int i = assignsBeforeTopOpRef.size() - 1; i >= 0; i--) {
                AssignOperator tmpOp = (AssignOperator) assignsBeforeTopOpRef.get(i).getValue();
                List<LogicalVariable> tmpAssignVars = tmpOp.getVariables();
                List<Mutable<ILogicalExpression>> tmpAsssignExprs = tmpOp.getExpressions();
                Iterator<LogicalVariable> varIt = tmpAssignVars.iterator();
                Iterator<Mutable<ILogicalExpression>> exprIt = tmpAsssignExprs.iterator();
                boolean changed = false;
                while (exprIt.hasNext()) {
                    Mutable<ILogicalExpression> tmpExpr = exprIt.next();
                    LogicalVariable tmpVar = varIt.next();
                    if (tmpExpr.getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                        constAssignVars.add(tmpVar);
                        constAssignExprs.add(tmpExpr);
                        varIt.remove();
                        exprIt.remove();
                        changed = true;
                    }
                }
                if (changed) {
                    context.computeAndSetTypeEnvironmentForOperator(tmpOp);
                }
            }

            if (!constAssignVars.isEmpty()) {
                // These constants should not be used in the SELECT or JOIN operator.
                for (LogicalVariable v : constAssignVars) {
                    if (usedVarsInTopOp.contains(v)) {
                        constantAssignVarUsedInTopOp = true;
                        break;
                    }
                }
                // If this assign operator is not used in the SELECT or JOIN operator,
                // we will add this operator after creating UNION operator in the last part of this method.
                constAssignOp = new AssignOperator(constAssignVars, constAssignExprs);
                constAssignOp.setSourceLocation(sourceLoc);
                if (constantAssignVarUsedInTopOp) {
                    // Places this assign after the secondary index-search op.
                    constAssignOp.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
                    constAssignOp.setExecutionMode(ExecutionMode.PARTITIONED);
                    context.computeAndSetTypeEnvironmentForOperator(constAssignOp);
                    currentOp = constAssignOp;
                }
            }
        }

        // variables used after SELECT or JOIN operator
        HashSet<LogicalVariable> varsTmpSet = new HashSet<>();
        if (afterTopOpRefs != null) {
            for (Mutable<ILogicalOperator> afterTopOpRef : afterTopOpRefs) {
                varsTmpSet.clear();
                OperatorPropertiesUtil.getFreeVariablesInOp(afterTopOpRef.getValue(), varsTmpSet);
                copyVarsToAnotherList(varsTmpSet, usedVarsAfterTopOp);
            }
        }

        // Now, adds a SPLIT operator to propagate <SK, PK> pair from the secondary-index search to the two paths.
        // And constructs the path from the secondary index search to the SPLIT operator.

        // Fetches the conditional split variable from the secondary-index search
        condSplitVars = AccessMethodUtils.getKeyVarsFromSecondaryUnnestMap(dataset, recordType, metaRecordType, inputOp,
                secondaryIndex, SecondaryUnnestMapOutputVarType.CONDITIONAL_SPLIT_VAR);

        // Adds a SPLIT operator after the given secondary index-search unnest-map operator.
        splitOp = new SplitOperator(2,
                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(condSplitVars.get(0))));
        splitOp.setSourceLocation(sourceLoc);
        splitOp.getInputs().add(new MutableObject<ILogicalOperator>(currentOp));
        splitOp.setExecutionMode(ExecutionMode.PARTITIONED);
        context.computeAndSetTypeEnvironmentForOperator(splitOp);

        // To maintain SSA, we assign new variables for the incoming variables in the left branch
        // since the most tuples go to the right branch (instantTryLock success path). Also, the output of
        // UNIONALL should be a new variable. (it cannot be the same to the left or right variable.)

        // Original variables (before SPLIT) to the variables in the left path mapping
        LinkedHashMap<LogicalVariable, LogicalVariable> liveVarAfterSplitToLeftPathMap = new LinkedHashMap<>();
        // output variables to the variables generated in the left branch mapping
        LinkedHashMap<LogicalVariable, LogicalVariable> origPKRecAndSKVarToleftPathMap = new LinkedHashMap<>();
        // Original variables (before SPLIT) to the output variables mapping (mainly for join case)
        LinkedHashMap<LogicalVariable, LogicalVariable> origVarToOutputVarMap = new LinkedHashMap<>();
        List<LogicalVariable> liveVarsAfterSplitOp = new ArrayList<>();
        VariableUtilities.getLiveVariables(splitOp, liveVarsAfterSplitOp);

        ArrayList<LogicalVariable> assignVars = new ArrayList<>();
        ArrayList<Mutable<ILogicalExpression>> assignExprs = new ArrayList<>();
        for (LogicalVariable v : liveVarsAfterSplitOp) {
            LogicalVariable newVar = context.newVar();
            liveVarAfterSplitToLeftPathMap.put(v, newVar);
            assignVars.add(newVar);
            VariableReferenceExpression vRef = new VariableReferenceExpression(v);
            vRef.setSourceLocation(sourceLoc);
            assignExprs.add(new MutableObject<ILogicalExpression>(vRef));
        }
        AssignOperator origVarsToLeftPathVarsAssignOp = new AssignOperator(assignVars, assignExprs);
        origVarsToLeftPathVarsAssignOp.setSourceLocation(sourceLoc);
        origVarsToLeftPathVarsAssignOp.getInputs().add(new MutableObject<ILogicalOperator>(splitOp));
        context.computeAndSetTypeEnvironmentForOperator(origVarsToLeftPathVarsAssignOp);
        origVarsToLeftPathVarsAssignOp.setExecutionMode(ExecutionMode.PARTITIONED);

        // Creates the variable mapping for the UNIONALL operator.

        // PK Variable(s) that will be fed into the primary index-search has been re-assigned in the left path.
        List<LogicalVariable> pkVarsInLeftPathFromSIdxSearchBeforeSplit = new ArrayList<>();
        for (LogicalVariable v : pkVarsFromSIdxUnnestMapOp) {
            pkVarsInLeftPathFromSIdxSearchBeforeSplit.add(liveVarAfterSplitToLeftPathMap.get(v));
        }
        // PK and Record variable(s) from the primary-index search will be reassigned in the left path
        // to make the output of the UNIONALL the original variables from the data-scan.
        List<LogicalVariable> pkVarsFromPIdxSearchInLeftPath = new ArrayList<>();
        for (int i = 0; i < primaryIndexUnnestVars.size(); i++) {
            LogicalVariable replacedVar = context.newVar();
            pkVarsFromPIdxSearchInLeftPath.add(replacedVar);
            origPKRecAndSKVarToleftPathMap.put(primaryIndexUnnestVars.get(i), replacedVar);
        }

        // Are the used variables after SELECT or JOIN operator from the primary index?
        // Then, creates the variable mapping between two paths.
        for (LogicalVariable tVar : usedVarsAfterTopOp) {
            // Checks whether this variable is already added to the union variable map.
            // It should be also a part of the primary key variables.
            if (findVarInTripleVarList(unionVarMap, tVar, false) || !primaryIndexUnnestVars.contains(tVar)) {
                continue;
            }
            int pIndexPKIdx = primaryIndexUnnestVars.indexOf(tVar);
            // If the above value is -1, either it is a secondary key variable or a variable
            // from different branch (join case). These cases will be dealt with later.
            if (pIndexPKIdx == -1) {
                continue;
            }
            unionVarMap.add(new Triple<>(pkVarsFromPIdxSearchInLeftPath.get(pIndexPKIdx),
                    pkVarsFromSIdxUnnestMapOp.get(pIndexPKIdx), tVar));
            origVarToOutputVarMap.put(pkVarsFromSIdxUnnestMapOp.get(pIndexPKIdx), tVar);

            // Constructs the mapping between the PK from the original data-scan to the PK
            // from the secondary index search since they are different logical variables.
            origVarToSIdxUnnestMapOpVarMap.put(tVar, pkVarsFromSIdxUnnestMapOp.get(pIndexPKIdx));
        }

        // Are the used variables after SELECT or JOIN operator from the given secondary index?
        for (LogicalVariable tVar : usedVarsAfterTopOp) {
            // Checks whether this variable is already added to the union variable map.
            if (findVarInTripleVarList(unionVarMap, tVar, false)) {
                continue;
            }
            // Should be either used in the condition or a composite index field that is not used in the condition.
            if (!usedVarsInTopOp.contains(tVar) && !producedVarsInAssignsBeforeTopOp.contains(tVar)) {
                continue;
            }
            int sIndexIdx = chosenIndexFieldNames.indexOf(subTree.getVarsToFieldNameMap().get(tVar));
            // For the join-case, the match might not exist.
            // In this case, we just propagate the variables later.
            if (sIndexIdx == -1) {
                continue;
            }
            if (idxType == IndexType.RTREE) {
                // R-Tree case: we need this variable in case if we need to do an additional verification,
                // or the secondary key field is used after SELECT or JOIN operator.
                // We need to use the re-constructed secondary key from the result (an MBR) of R-Tree search.
                // For the join case, the match might not exist.
                // In this case, we just propagate the variables later.
                if (!skFieldUsedAfterTopOp && !requireVerificationAfterSIdxSearch) {
                    continue;
                }
                LogicalVariable replacedVar = context.newVar();
                origPKRecAndSKVarToleftPathMap.put(tVar, replacedVar);
                origSKFieldVarToNewSKFieldVarMap.put(tVar, restoredSKVarFromRTree.get(sIndexIdx));
                unionVarMap.add(new Triple<>(replacedVar, restoredSKVarFromRTree.get(sIndexIdx), tVar));
                continue;
            }
            // B-Tree case:
            LogicalVariable replacedVar = context.newVar();
            origPKRecAndSKVarToleftPathMap.put(tVar, replacedVar);
            origVarToOutputVarMap.put(skVarsFromSIdxUnnestMap.get(sIndexIdx), tVar);
            unionVarMap.add(new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(replacedVar,
                    skVarsFromSIdxUnnestMap.get(sIndexIdx), tVar));
            // Constructs the mapping between the SK from the original data-scan
            // and the SK from the secondary index search since they are different logical variables.
            origVarToSIdxUnnestMapOpVarMap.put(tVar, skVarsFromSIdxUnnestMap.get(sIndexIdx));
        }

        // For B-Tree case: if the given secondary key field variable is used only in the select or
        // join condition, we were not able to catch the mapping between the the SK from the original
        // data-scan and the SK from the secondary index search since they are different logical variables.
        // (E.g., we are sending a query on a composite index but returns only one field.)
        List<LogicalVariable> varsUsedInTopOpButNotAfterwards = new ArrayList<>();
        copyVarsToAnotherList(uniqueUsedVarsInTopOp, varsUsedInTopOpButNotAfterwards);
        varsUsedInTopOpButNotAfterwards.removeAll(usedVarsAfterTopOp);
        if (idxType == IndexType.BTREE) {
            for (LogicalVariable v : varsUsedInTopOpButNotAfterwards) {
                int sIndexIdx = chosenIndexFieldNames.indexOf(subTree.getVarsToFieldNameMap().get(v));
                // For the join-case, the match might not exist.
                // In this case, we just propagate the variables later.
                if (sIndexIdx == -1) {
                    continue;
                }
                LogicalVariable replacedVar = context.newVar();
                origPKRecAndSKVarToleftPathMap.put(v, replacedVar);
                origVarToOutputVarMap.put(skVarsFromSIdxUnnestMap.get(sIndexIdx), v);
                // Constructs the mapping between the SK from the original data-scan
                // and the SK from the secondary index search since they are different logical variables.
                origVarToSIdxUnnestMapOpVarMap.put(v, skVarsFromSIdxUnnestMap.get(sIndexIdx));
            }
        }

        // For R-Tree case: if the given secondary key field variable is used only in the select or join condition,
        // we were not able to catch the mapping between the original secondary key field and the newly restored
        // secondary key field in the assign operator in the right path.
        if (idxType == IndexType.RTREE && (skFieldUsedAfterTopOp || requireVerificationAfterSIdxSearch)) {
            for (LogicalVariable v : uniqueUsedVarsInTopOp) {
                if (!primaryIndexUnnestVars.contains(v)) {
                    origSKFieldVarToNewSKFieldVarMap.put(v, restoredSKVarFromRTree.get(0));
                }
            }
        }

        // For the index-nested-loop join case,
        // we propagate all variables that come from the outer relation and are used after join operator.
        // Adds the variables that are both live after JOIN and used after the JOIN operator.
        VariableUtilities.getLiveVariables(topOpRef.getValue(), liveVarsAfterTopOp);
        for (LogicalVariable v : usedVarsAfterTopOp) {
            if (!liveVarsAfterTopOp.contains(v) || findVarInTripleVarList(unionVarMap, v, false)) {
                continue;
            }
            LogicalVariable outputVar = context.newVar();
            origVarToOutputVarMap.put(v, outputVar);
            unionVarMap.add(new Triple<>(liveVarAfterSplitToLeftPathMap.get(v), v, outputVar));
        }

        // Replaces the original variables in the operators after the SELECT or JOIN operator to satisfy SSA.
        if (afterTopOpRefs != null) {
            for (Mutable<ILogicalOperator> afterTopOpRef : afterTopOpRefs) {
                VariableUtilities.substituteVariables(afterTopOpRef.getValue(), origVarToOutputVarMap, context);
            }
        }

        // Creates the primary index lookup operator.
        // The job gen parameters are transferred to the actual job gen via the UnnestMapOperator's function arguments.
        AbstractUnnestMapOperator primaryIndexUnnestMapOp = createPrimaryIndexUnnestMapOp(dataset, retainInput,
                retainMissing, requiresBroadcast, pkVarsInLeftPathFromSIdxSearchBeforeSplit,
                pkVarsFromPIdxSearchInLeftPath, primaryIndexOutputTypes, sourceLoc);
        primaryIndexUnnestMapOp.setSourceLocation(sourceLoc);
        primaryIndexUnnestMapOp.getInputs().add(new MutableObject<ILogicalOperator>(origVarsToLeftPathVarsAssignOp));
        context.computeAndSetTypeEnvironmentForOperator(primaryIndexUnnestMapOp);
        primaryIndexUnnestMapOp.setExecutionMode(ExecutionMode.PARTITIONED);

        // Now, generates the UnionAllOperator to merge the left and right paths.
        // If we are transforming a join, in the instantTryLock on PK fail path, a SELECT operator should be
        // constructed from the join condition and placed after the primary index lookup
        // to do the final verification. If this is a select plan, we just need to use the original
        // SELECT operator after the primary index lookup to do the final verification.
        LinkedHashMap<LogicalVariable, LogicalVariable> origVarToNewVarInLeftPathMap = new LinkedHashMap<>();
        origVarToNewVarInLeftPathMap.putAll(liveVarAfterSplitToLeftPathMap);
        origVarToNewVarInLeftPathMap.putAll(origPKRecAndSKVarToleftPathMap);
        ILogicalExpression conditionRefExpr = conditionRef.getValue().cloneExpression();
        // The retainMissing variable contains the information whether we are optimizing a left-outer join or not.
        LogicalVariable newMissingPlaceHolderVar = retainMissing ? newMissingPlaceHolderForLOJ : null;
        newSelectOpInLeftPath = new SelectOperator(new MutableObject<ILogicalExpression>(conditionRefExpr),
                retainMissing, newMissingPlaceHolderVar);
        newSelectOpInLeftPath.setSourceLocation(conditionRefExpr.getSourceLocation());
        VariableUtilities.substituteVariables(newSelectOpInLeftPath, origVarToNewVarInLeftPathMap, context);

        // If there are ASSIGN operators before the SELECT or JOIN operator,
        // we need to put these operators between the SELECT or JOIN and the primary index lookup in the left path.
        if (assignsBeforeTopOpRef != null) {
            // Makes the primary unnest-map as the child of the last ASSIGN (from top) in the path.
            assignBeforeTopOp = assignsBeforeTopOpRef.get(assignsBeforeTopOpRef.size() - 1).getValue();
            assignBeforeTopOp.getInputs().clear();
            assignBeforeTopOp.getInputs().add(new MutableObject<ILogicalOperator>(primaryIndexUnnestMapOp));

            // Makes the first ASSIGN (from top) as the child of the SELECT operator.
            for (int i = assignsBeforeTopOpRef.size() - 1; i >= 0; i--) {
                if (assignsBeforeTopOpRef.get(i) != null) {
                    AbstractLogicalOperator assignTmpOp =
                            (AbstractLogicalOperator) assignsBeforeTopOpRef.get(i).getValue();
                    assignTmpOp.setExecutionMode(ExecutionMode.PARTITIONED);
                    VariableUtilities.substituteVariables(assignTmpOp, origVarToNewVarInLeftPathMap, context);
                    context.computeAndSetTypeEnvironmentForOperator(assignTmpOp);
                }
            }
            newSelectOpInLeftPath.getInputs().clear();
            newSelectOpInLeftPath.getInputs()
                    .add(new MutableObject<ILogicalOperator>(assignsBeforeTopOpRef.get(0).getValue()));
        } else {
            newSelectOpInLeftPath.getInputs().add(new MutableObject<ILogicalOperator>(primaryIndexUnnestMapOp));
        }
        newSelectOpInLeftPath.setExecutionMode(ExecutionMode.PARTITIONED);
        context.computeAndSetTypeEnvironmentForOperator(newSelectOpInLeftPath);

        // Now, we take care of the right path (instantTryLock on PK success path).
        ILogicalOperator currentTopOpInRightPath = splitOp;
        // For an R-Tree index, if there are operators that are using the secondary key field value,
        // we need to reconstruct that field value from the result of R-Tree search.
        // This is done by adding the following assign operator that we have made in the beginning of this method.
        if (skVarAssignOpInRightPath != null) {
            skVarAssignOpInRightPath.getInputs().add(new MutableObject<ILogicalOperator>(splitOp));
            skVarAssignOpInRightPath.setExecutionMode(ExecutionMode.PARTITIONED);
            context.computeAndSetTypeEnvironmentForOperator(skVarAssignOpInRightPath);
            currentTopOpInRightPath = skVarAssignOpInRightPath;
        }

        // For an R-Tree index, if the given query shape is not RECTANGLE or POINT,
        // we need to add the original SELECT operator to filter out the false positive results.
        // (e.g., spatial-intersect($o.pointfield, create-circle(create-point(30.0,70.0), 5.0)) )
        //
        // Also, for a B-Tree composite index, we need to apply SELECT operators in the right path
        // to remove any false positive results from the secondary composite index search.
        //
        // Lastly, if there is an index-nested-loop-join and the join contains more conditions
        // other than joining fields, then those conditions need to be applied to filter out
        // false positive results in the right path.
        // (e.g., where $a.authors /*+ indexnl */ = $b.authors and $a.id = $b.id   <- authors:SK, id:PK)
        if ((idxType == IndexType.RTREE || uniqueUsedVarsInTopOp.size() > 1) && requireVerificationAfterSIdxSearch) {
            // Creates a new SELECT operator by deep-copying the SELECT operator in the left path
            // since we need to change the variable reference in the SELECT operator.
            // For the index-nested-loop join case, we copy the condition of the join operator.
            ILogicalExpression conditionRefExpr2 = conditionRef.getValue().cloneExpression();
            newSelectOpInRightPath = new SelectOperator(new MutableObject<ILogicalExpression>(conditionRefExpr2),
                    retainMissing, newMissingPlaceHolderVar);
            newSelectOpInRightPath.setSourceLocation(conditionRefExpr2.getSourceLocation());
            newSelectOpInRightPath.getInputs().add(new MutableObject<ILogicalOperator>(currentTopOpInRightPath));
            VariableUtilities.substituteVariables(newSelectOpInRightPath, origVarToSIdxUnnestMapOpVarMap, context);
            VariableUtilities.substituteVariables(newSelectOpInRightPath, origSKFieldVarToNewSKFieldVarMap, context);
            newSelectOpInRightPath.setExecutionMode(ExecutionMode.PARTITIONED);
            context.computeAndSetTypeEnvironmentForOperator(newSelectOpInRightPath);
            currentTopOpInRightPath = newSelectOpInRightPath;
        }

        // Adds the new missing place holder in case of a left-outer-join if it's not been added yet.
        // The assumption here is that this variable is the first PK variable that was set.
        if (retainMissing && newMissingPlaceHolderForLOJ == primaryIndexUnnestVars.get(0)
                && !findVarInTripleVarList(unionVarMap, newMissingPlaceHolderForLOJ, false)) {
            unionVarMap.add(new Triple<>(origPKRecAndSKVarToleftPathMap.get(newMissingPlaceHolderForLOJ),
                    pkVarsFromSIdxUnnestMapOp.get(0), newMissingPlaceHolderForLOJ));
        }

        // UNIONALL operator that combines both paths.
        unionAllOp = new UnionAllOperator(unionVarMap);
        unionAllOp.setSourceLocation(sourceLoc);
        unionAllOp.getInputs().add(new MutableObject<ILogicalOperator>(newSelectOpInLeftPath));
        unionAllOp.getInputs().add(new MutableObject<ILogicalOperator>(currentTopOpInRightPath));

        unionAllOp.setExecutionMode(ExecutionMode.PARTITIONED);
        context.computeAndSetTypeEnvironmentForOperator(unionAllOp);

        // If an assign operator that keeps constant values was added, set the UNIONALL operator as its child.
        if (!constAssignVars.isEmpty() && !constantAssignVarUsedInTopOp) {
            constAssignOp.getInputs().clear();
            constAssignOp.getInputs().add(new MutableObject<ILogicalOperator>(unionAllOp));
            constAssignOp.setExecutionMode(ExecutionMode.PARTITIONED);
            context.computeAndSetTypeEnvironmentForOperator(constAssignOp);

            // This constant assign operator is the new child of the first operator after the original
            // SELECT or JOIN operator.
            currentOpAfterTopOp = afterTopOpRefs.get(afterTopOpRefs.size() - 1).getValue();
            currentOpAfterTopOp.getInputs().clear();
            currentOpAfterTopOp.getInputs().add(new MutableObject<ILogicalOperator>(constAssignOp));
            context.computeAndSetTypeEnvironmentForOperator(currentOpAfterTopOp);
            afterTopOpRefs.add(new MutableObject<ILogicalOperator>(constAssignOp));
        }

        // Index-only plan is now constructed. Return this operator to the caller.
        return unionAllOp;
    }

    private static AbstractUnnestMapOperator createPrimaryIndexUnnestMapOp(Dataset dataset, boolean retainInput,
            boolean retainMissing, boolean requiresBroadcast, List<LogicalVariable> primaryKeyVars,
            List<LogicalVariable> primaryIndexUnnestVars, List<Object> primaryIndexOutputTypes,
            SourceLocation sourceLoc) throws AlgebricksException {
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
        // An index search is expressed as an unnest over an index-search function.
        IFunctionInfo primaryIndexSearch = FunctionUtil.getFunctionInfo(BuiltinFunctions.INDEX_SEARCH);
        AbstractFunctionCallExpression primaryIndexSearchFunc =
                new ScalarFunctionCallExpression(primaryIndexSearch, primaryIndexFuncArgs);
        primaryIndexSearchFunc.setSourceLocation(sourceLoc);
        // This is the operator that jobgen will be looking for. It contains an unnest function that has
        // all necessary arguments to determine which index to use, which variables contain the index-search keys,
        // what is the original dataset, etc.
        AbstractUnnestMapOperator primaryIndexUnnestMapOp = null;
        if (retainMissing) {
            if (retainInput) {
                primaryIndexUnnestMapOp = new LeftOuterUnnestMapOperator(primaryIndexUnnestVars,
                        new MutableObject<ILogicalExpression>(primaryIndexSearchFunc), primaryIndexOutputTypes,
                        retainInput);
                primaryIndexUnnestMapOp.setSourceLocation(sourceLoc);
            } else {
                // Left-outer-join without retainNull and retainInput doesn't make sense.
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                        "Left-outer-join should propagate all inputs from the outer branch.");
            }
        } else {
            primaryIndexUnnestMapOp = new UnnestMapOperator(primaryIndexUnnestVars,
                    new MutableObject<ILogicalExpression>(primaryIndexSearchFunc), primaryIndexOutputTypes,
                    retainInput);
            primaryIndexUnnestMapOp.setSourceLocation(sourceLoc);
        }
        return primaryIndexUnnestMapOp;
    }

    /**
     * Creates operators that do a primary index lookup in the plan. In case of an index-only plan,
     * this creates two paths including the primary index lookup in the left path.
     * If this is an index-only plan (only using PK and/or secondary field(s) after SELECT operator) and/or
     * the combination of the SELECT (JOIN) condition and the chosen secondary index do not generate
     * false positive results, we can apply instantTryLock() on PK optimization since a result from these indexes
     * doesn't have to be verified by the primary index-lookup and a subsequent SELECT operator.
     * (i.e., we can guarantee the correctness of the result.)
     *
     * Case A) non-index-only plan
     * sidx-search -> (optional) sort -> pdix-search
     *
     * Case B) index-only plan
     * left path (an instantTryLock() on the PK fail path):
     * right path(an instantTryLock() on the PK success path):
     * (left) sidx-search -> assign? -> split -> primary index-search -> select (verification) -> union ->
     * (right) ........................ split -> assign? -> select? -> .......................... union ...
     */
    public static ILogicalOperator createRestOfIndexSearchPlan(List<Mutable<ILogicalOperator>> afterTopOpRefs,
            Mutable<ILogicalOperator> topOpRef, Mutable<ILogicalExpression> conditionRef,
            List<Mutable<ILogicalOperator>> assignsBeforeTopOpRef, AbstractDataSourceOperator dataSourceOp,
            Dataset dataset, ARecordType recordType, ARecordType metaRecordType, ILogicalOperator inputOp,
            IOptimizationContext context, boolean sortPrimaryKeys, boolean retainInput, boolean retainMissing,
            boolean requiresBroadcast, Index secondaryIndex, AccessMethodAnalysisContext analysisCtx,
            OptimizableOperatorSubTree subTree, LogicalVariable newMissingPlaceHolderForLOJ)
            throws AlgebricksException {
        // Common part for the non-index-only plan and index-only plan
        // Variables and types for the primary-index search.
        List<LogicalVariable> primaryIndexUnnestVars = new ArrayList<>();
        List<Object> primaryIndexOutputTypes = new ArrayList<Object>();
        // Appends output variables/types generated by the primary-index search (not forwarded from input).
        primaryIndexUnnestVars.addAll(dataSourceOp.getVariables());
        appendPrimaryIndexTypes(dataset, recordType, metaRecordType, primaryIndexOutputTypes);

        // Fetches PK variable(s) from the secondary-index search operator.
        List<LogicalVariable> pkVarsFromSIdxUnnestMapOp = AccessMethodUtils.getKeyVarsFromSecondaryUnnestMap(dataset,
                recordType, metaRecordType, inputOp, secondaryIndex, SecondaryUnnestMapOutputVarType.PRIMARY_KEY);

        // Index-only plan or not?
        boolean isIndexOnlyPlan = analysisCtx.getIndexOnlyPlanInfo().getFirst();

        // Non-index-only plan case: creates ORDER -> UNNEST-MAP(Primary-index search) and return that unnest-map op.
        if (!isIndexOnlyPlan) {
            return createFinalNonIndexOnlySearchPlan(dataset, inputOp, context, sortPrimaryKeys, retainInput,
                    retainMissing, requiresBroadcast, pkVarsFromSIdxUnnestMapOp, primaryIndexUnnestVars,
                    primaryIndexOutputTypes);
        } else {
            // Index-only plan case: creates a UNIONALL operator that has two paths after the secondary unnest-map op,
            // and returns it.
            return createFinalIndexOnlySearchPlan(afterTopOpRefs, topOpRef, conditionRef, assignsBeforeTopOpRef,
                    dataset, recordType, metaRecordType, inputOp, context, retainInput, retainMissing,
                    requiresBroadcast, secondaryIndex, analysisCtx, subTree, newMissingPlaceHolderForLOJ,
                    pkVarsFromSIdxUnnestMapOp, primaryIndexUnnestVars, primaryIndexOutputTypes);
        }
    }

    private static AbstractFunctionCallExpression createPointExpression(List<LogicalVariable> pointVars,
            SourceLocation sourceLoc) {
        List<Mutable<ILogicalExpression>> expressions = new ArrayList<>();
        AbstractFunctionCallExpression createPointExpr1 =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.CREATE_POINT));
        createPointExpr1.setSourceLocation(sourceLoc);
        VariableReferenceExpression pointVarRef0 = new VariableReferenceExpression(pointVars.get(0));
        pointVarRef0.setSourceLocation(sourceLoc);
        expressions.add(new MutableObject<ILogicalExpression>(pointVarRef0));
        VariableReferenceExpression pointVarRef1 = new VariableReferenceExpression(pointVars.get(1));
        pointVarRef1.setSourceLocation(sourceLoc);
        expressions.add(new MutableObject<ILogicalExpression>(pointVarRef1));
        createPointExpr1.getArguments().addAll(expressions);
        return createPointExpr1;
    }

    private static AbstractFunctionCallExpression createRectangleExpression(
            AbstractFunctionCallExpression createPointExpr1, AbstractFunctionCallExpression createPointExpr2) {
        List<Mutable<ILogicalExpression>> expressions = new ArrayList<>();
        AbstractFunctionCallExpression createRectangleExpr =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.CREATE_RECTANGLE));
        createRectangleExpr.setSourceLocation(createPointExpr1.getSourceLocation());
        expressions.add(new MutableObject<ILogicalExpression>(createPointExpr1));
        expressions.add(new MutableObject<ILogicalExpression>(createPointExpr2));
        createRectangleExpr.getArguments().addAll(expressions);
        return createRectangleExpr;
    }

    private static ScalarFunctionCallExpression getNestedIsMissingCall(AbstractFunctionCallExpression call,
            OptimizableOperatorSubTree rightSubTree) throws AlgebricksException {
        ScalarFunctionCallExpression isMissingFuncExpr;
        if (call.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.NOT)) {
            if (call.getArguments().get(0).getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                if (((AbstractFunctionCallExpression) call.getArguments().get(0).getValue()).getFunctionIdentifier()
                        .equals(AlgebricksBuiltinFunctions.IS_MISSING)) {
                    isMissingFuncExpr = (ScalarFunctionCallExpression) call.getArguments().get(0).getValue();
                    if (isMissingFuncExpr.getArguments().get(0).getValue()
                            .getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                        LogicalVariable var =
                                ((VariableReferenceExpression) isMissingFuncExpr.getArguments().get(0).getValue())
                                        .getVariableReference();
                        List<LogicalVariable> liveSubplanVars = new ArrayList<>();
                        VariableUtilities.getSubplanLocalLiveVariables(rightSubTree.getRoot(), liveSubplanVars);
                        if (liveSubplanVars.contains(var)) {
                            return isMissingFuncExpr;
                        }
                    }
                }
            }
        }
        return null;
    }

    public static ScalarFunctionCallExpression findIsMissingInSubplan(AbstractLogicalOperator inputOp,
            OptimizableOperatorSubTree rightSubTree) throws AlgebricksException {
        ScalarFunctionCallExpression isMissingFuncExpr = null;
        AbstractLogicalOperator currentOp = inputOp;
        while (currentOp != null) {
            if (currentOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
                SelectOperator selectOp = (SelectOperator) currentOp;
                if (selectOp.getCondition().getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    AbstractFunctionCallExpression call =
                            (AbstractFunctionCallExpression) (selectOp).getCondition().getValue();
                    if (call.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.AND)) {
                        for (Mutable<ILogicalExpression> mexpr : call.getArguments()) {
                            if (mexpr.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                                isMissingFuncExpr = getNestedIsMissingCall(
                                        (AbstractFunctionCallExpression) mexpr.getValue(), rightSubTree);
                                if (isMissingFuncExpr != null) {
                                    return isMissingFuncExpr;
                                }
                            }
                        }
                    }
                    isMissingFuncExpr = getNestedIsMissingCall(call, rightSubTree);
                    if (isMissingFuncExpr != null) {
                        return isMissingFuncExpr;
                    }
                }
            } else if (currentOp.hasNestedPlans()) {
                AbstractOperatorWithNestedPlans nestedPlanOp = (AbstractOperatorWithNestedPlans) currentOp;
                for (ILogicalPlan nestedPlan : nestedPlanOp.getNestedPlans()) {
                    for (Mutable<ILogicalOperator> root : nestedPlan.getRoots()) {
                        isMissingFuncExpr =
                                findIsMissingInSubplan((AbstractLogicalOperator) root.getValue(), rightSubTree);
                        if (isMissingFuncExpr != null) {
                            return isMissingFuncExpr;
                        }
                    }
                }
            }
            currentOp = currentOp.getInputs().isEmpty() ? null
                    : (AbstractLogicalOperator) currentOp.getInputs().get(0).getValue();
        }
        return isMissingFuncExpr;
    }

    public static ScalarFunctionCallExpression findLOJIsMissingFuncInGroupBy(GroupByOperator lojGroupbyOp,
            OptimizableOperatorSubTree rightSubTree) throws AlgebricksException {
        //find IS_MISSING function of which argument has the nullPlaceholder variable in the nested plan of groupby.
        ALogicalPlanImpl subPlan = (ALogicalPlanImpl) lojGroupbyOp.getNestedPlans().get(0);
        Mutable<ILogicalOperator> subPlanRootOpRef = subPlan.getRoots().get(0);
        AbstractLogicalOperator subPlanRootOp = (AbstractLogicalOperator) subPlanRootOpRef.getValue();
        ScalarFunctionCallExpression isMissingFuncExpr = findIsMissingInSubplan(subPlanRootOp, rightSubTree);

        if (isMissingFuncExpr == null) {
            throw CompilationException.create(ErrorCode.CANNOT_FIND_NON_MISSING_SELECT_OPERATOR,
                    lojGroupbyOp.getSourceLocation());
        }
        return isMissingFuncExpr;
    }

    public static void resetLOJMissingPlaceholderVarInGroupByOp(AccessMethodAnalysisContext analysisCtx,
            LogicalVariable newMissingPlaceholderVaraible, IOptimizationContext context) throws AlgebricksException {

        //reset the missing placeholder variable in groupby operator
        ScalarFunctionCallExpression isMissingFuncExpr = analysisCtx.getLOJIsMissingFuncInGroupBy();
        isMissingFuncExpr.getArguments().clear();
        VariableReferenceExpression newMissingVarRef = new VariableReferenceExpression(newMissingPlaceholderVaraible);
        newMissingVarRef.setSourceLocation(isMissingFuncExpr.getSourceLocation());
        isMissingFuncExpr.getArguments().add(new MutableObject<ILogicalExpression>(newMissingVarRef));

        //recompute type environment.
        OperatorPropertiesUtil.typeOpRec(analysisCtx.getLOJGroupbyOpRef(), context);
    }

    // New < For external datasets indexing>
    private static void appendExternalRecTypes(Dataset dataset, IAType itemType, List<Object> target) {
        target.add(itemType);
    }

    private static void appendExternalRecPrimaryKeys(Dataset dataset, List<Object> target) throws AlgebricksException {
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
            Dataset dataset, ARecordType recordType, ARecordType metaRecordType, ILogicalOperator inputOp,
            IOptimizationContext context, Index secondaryIndex, boolean retainInput, boolean retainNull)
            throws AlgebricksException {
        SourceLocation sourceLoc = inputOp.getSourceLocation();
        List<LogicalVariable> primaryKeyVars = AccessMethodUtils.getKeyVarsFromSecondaryUnnestMap(dataset, recordType,
                metaRecordType, inputOp, secondaryIndex, SecondaryUnnestMapOutputVarType.PRIMARY_KEY);
        // add a sort on the RID fields before fetching external data.
        OrderOperator order = new OrderOperator();
        order.setSourceLocation(sourceLoc);
        for (LogicalVariable pkVar : primaryKeyVars) {
            VariableReferenceExpression pkVarRef = new VariableReferenceExpression(pkVar);
            pkVarRef.setSourceLocation(sourceLoc);
            Mutable<ILogicalExpression> vRef = new MutableObject<>(pkVarRef);
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
        externalLookupFunc.setSourceLocation(sourceLoc);
        UnnestMapOperator unnestOp = new UnnestMapOperator(externalUnnestVars,
                new MutableObject<ILogicalExpression>(externalLookupFunc), outputTypes, retainInput);
        unnestOp.setSourceLocation(sourceLoc);
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

    /**
     * Checks whether the given function can generate false-positive results when using a corresponding index type.
     */
    public static Pair<Boolean, Boolean> canFunctionGenerateFalsePositiveResultsUsingIndex(
            AbstractFunctionCallExpression funcExpr, List<Pair<FunctionIdentifier, Boolean>> funcIdents) {
        boolean requireVerificationAfterSIdxSearch = true;

        // Check whether the given function-call can generate false positive results.
        FunctionIdentifier argFuncIdent = funcExpr.getFunctionIdentifier();
        boolean functionFound = false;
        for (int i = 0; i < funcIdents.size(); i++) {
            if (argFuncIdent.equals(funcIdents.get(i).first)) {
                functionFound = true;
                requireVerificationAfterSIdxSearch = funcIdents.get(i).second;
                break;
            }
        }

        // If function-call itself is not an index-based access method, we check its arguments.
        if (!functionFound) {
            for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
                ILogicalExpression argExpr = arg.getValue();
                if (argExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                    continue;
                }
                AbstractFunctionCallExpression argFuncExpr = (AbstractFunctionCallExpression) argExpr;
                FunctionIdentifier argExprFuncIdent = argFuncExpr.getFunctionIdentifier();
                for (int i = 0; i < funcIdents.size(); i++) {
                    if (argExprFuncIdent.equals(funcIdents.get(i).first)) {
                        functionFound = true;
                        requireVerificationAfterSIdxSearch = funcIdents.get(i).second;
                        break;
                    }
                }
            }
        }

        return new Pair<>(functionFound, requireVerificationAfterSIdxSearch);
    }

    /**
     * Checks whether the given plan is an index-only plan (a.k.a. instantTryLock() on PK optimization).
     * Refer to the IntroduceSelectAccessMethodRule or IntroduceJoinAccessMethodRule for more details.
     *
     * @throws AlgebricksException
     */
    public static void indexOnlyPlanCheck(List<Mutable<ILogicalOperator>> afterTopRefs,
            Mutable<ILogicalOperator> topRef, OptimizableOperatorSubTree indexSubTree,
            OptimizableOperatorSubTree probeSubTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, Quadruple<Boolean, Boolean, Boolean, Boolean> indexOnlyPlanInfo)
            throws AlgebricksException {
        // First, checks all cases that the index-only can't be applied. If so, we can stop here.
        Dataset dataset = indexSubTree.getDataset();
        // For an external dataset, primary index, we don't apply index-only plan optimization.
        // For a non-enforced index, we also don't apply index-only plan since it can contain a casted numeric value.
        // For an enforced index, we also don't apply the index-only pan since the key field from a secondary index
        // may not be equal to the actual value in the record. (e.g., INT index and BIGINT value in the actual record)
        // Since index-only plan doesn't access the primary index, we can't get the actual value in this case.
        // Also, if no-index-only option is given, we stop here to honor that request.
        boolean noIndexOnlyPlanOption = getNoIndexOnlyOption(context);
        // TODO: For the inverted index access-method cases only:
        // Since an inverted index can contain multiple secondary key entries per one primary key,
        // Index-only plan can't be applied. For example, suppose there are two entries (SK1, SK2) for one PK.
        // Since we can't access <SK1, PK>, <SK2, PK> at the same time unless we use tryLock (we use instantTryLock),
        // right now, we can't support an index-only plan on an inverted index.
        // Once this issue is resolved, we can apply an index-only plan.
        // One additional condition:
        // Even if the above is resolved, if a secondary key field is used after
        // SELECT or JOIN operator, this can't be qualified as an index-only plan since
        // an inverted index contains a part of a field value, not all of it.
        if (dataset.getDatasetType() == DatasetType.EXTERNAL || chosenIndex.isPrimaryIndex()
                || chosenIndex.isOverridingKeyFieldTypes() || chosenIndex.isEnforced() || isInvertedIndex(chosenIndex)
                || noIndexOnlyPlanOption) {
            indexOnlyPlanInfo.setFirst(false);
            return;
        }

        // index-only plan possible?
        boolean isIndexOnlyPlan = false;

        // secondary key field usage after the select (join) operators
        // This boolean is mainly used for R-Tree case since R-Tree index generates an MBR
        // and we can restore original point or rectangle from this MBR if an index is built on point or rectangle.
        boolean secondaryKeyFieldUsedAfterSelectOrJoinOp;

        // Whether a post verification (especially for R-Tree case) is required after the secondary index search
        // (e.g., the shape of the given query is not a point or rectangle.
        //        Then, we may need to apply the select again using the real polygon, not MBR of it to get the true
        //        result, not a super-set of it.)
        boolean requireVerificationAfterSIdxSearch = indexOnlyPlanInfo.getThird();

        // Does the given index can cover all search predicates?
        boolean doesSIdxSearchCoverAllPredicates;

        // matched function expressions
        List<IOptimizableFuncExpr> matchedFuncExprs = analysisCtx.getMatchedFuncExprs();

        // logical variables that select (join) operator is using
        List<LogicalVariable> usedVarsInSelJoinOp = new ArrayList<>();
        List<LogicalVariable> usedVarsInSelJoinOpTemp = new ArrayList<>();

        // live variables that select (join) operator can access
        List<LogicalVariable> liveVarsAfterSelJoinOp = new ArrayList<>();

        // PK, record variable
        List<LogicalVariable> dataScanPKRecordVars;
        List<LogicalVariable> dataScanPKVars = new ArrayList<>();
        List<LogicalVariable> dataScanRecordVars = new ArrayList<>();

        // Collects the used variables in the given select (join) operator.
        VariableUtilities.getUsedVariables(topRef.getValue(), usedVarsInSelJoinOpTemp);

        // Removes the duplicated variables that are used in the select (join) operator
        // in case where the variable is used multiple times in the operator's expression.
        // (e.g., $i < 100 and $i > 10)
        copyVarsToAnotherList(usedVarsInSelJoinOpTemp, usedVarsInSelJoinOp);

        // If this is a join, we need to traverse the index subtree and find possible SELECT conditions
        // since there may be more SELECT conditions and we need to collect used variables.
        List<LogicalVariable> selectInIndexSubTreeVars = new ArrayList<>();
        if (probeSubTree != null) {
            ILogicalOperator tmpOp = indexSubTree.getRoot();
            while (tmpOp.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
                if (tmpOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
                    VariableUtilities.getUsedVariables(tmpOp, selectInIndexSubTreeVars);
                    // Remove any duplicated variables.
                    copyVarsToAnotherList(selectInIndexSubTreeVars, usedVarsInSelJoinOp);
                    selectInIndexSubTreeVars.clear();
                }
                tmpOp = tmpOp.getInputs().get(0).getValue();
            }
        }
        usedVarsInSelJoinOpTemp.clear();

        // For the index-nested-loop join case, we need to remove variables from the left (outer) relation
        // from used variable checking after join operator.
        // This is because these variables are already generated from the outer branch and
        // these variables are not related to the decision whether the plan is an index-only plan or not.
        // Only the variables from the right (inner) branch matter to the decision process since an index-search
        // will be placed on the right (inner) branch.
        List<LogicalVariable> liveVarsInSubTreeRootOp = new ArrayList<>();
        List<LogicalVariable> producedVarsInSubTreeRootOp = new ArrayList<>();

        VariableUtilities.getLiveVariables(indexSubTree.getRootRef().getValue(), liveVarsInSubTreeRootOp);
        VariableUtilities.getProducedVariables(indexSubTree.getRootRef().getValue(), producedVarsInSubTreeRootOp);

        copyVarsToAnotherList(liveVarsInSubTreeRootOp, liveVarsAfterSelJoinOp);
        copyVarsToAnotherList(producedVarsInSubTreeRootOp, liveVarsAfterSelJoinOp);

        // Excludes the variables from the outer branch - in a join case.
        for (Iterator<LogicalVariable> iterator = usedVarsInSelJoinOp.iterator(); iterator.hasNext();) {
            LogicalVariable v = iterator.next();
            if (!liveVarsAfterSelJoinOp.contains(v)) {
                iterator.remove();
            }
        }

        // Gets the PK, record variables from the index subtree.
        dataScanPKRecordVars = indexSubTree.getDataSourceVariables();
        // On an external dataset, there is no PK.
        if (dataScanPKRecordVars.size() > 1) {
            indexSubTree.getPrimaryKeyVars(null, dataScanPKVars);
        }
        dataScanRecordVars.addAll(dataScanPKRecordVars);
        dataScanRecordVars.removeAll(dataScanPKVars);

        // We know that this plan is utilizing an index, however we are not sure
        // whether this plan is an index-only plan.
        // Thus, we check whether the given select (join) operator is using only variables from
        // assign or data-source-scan in the subtree and the field-name of those variables are only PK or SK.
        // Needs to check whether variables from the given select (join) operator only contain SK and/or PK condition.
        List<List<String>> pkFieldNames = dataset.getPrimaryKeys();
        List<List<String>> chosenIndexFieldNames = chosenIndex.getKeyFieldNames();
        List<LogicalVariable> chosenIndexVars = new ArrayList<>();

        // Collects variables that contain a CONSTANT expression in ASSIGN operators in the subtree.
        // This is needed to skip the data origin check for these variables since they are constants.
        List<LogicalVariable> constantVars = new ArrayList<>();
        getConstantVars(indexSubTree.getAssignsAndUnnests(), constantVars);

        // #1. The first step: checks whether variables in the given SELECT (JOIN) operator
        // are only using the secondary key fields and/or PK fields.
        isIndexOnlyPlan = checkVarUsageInSelectOrJoinCondExpr(matchedFuncExprs, usedVarsInSelJoinOp, pkFieldNames,
                chosenIndexFieldNames, chosenIndexVars);
        int indexApplicableVarFoundCount = chosenIndexVars.size();

        // All variables in the SELECT or JOIN condition should be found and counted.
        if (indexApplicableVarFoundCount < usedVarsInSelJoinOp.size()) {
            isIndexOnlyPlan = false;
            // The given index can't cover all search predicates.
            doesSIdxSearchCoverAllPredicates = false;
        } else {
            doesSIdxSearchCoverAllPredicates = true;
        }

        // For the composite index, a secondary-index search generates a superset of the results.
        if (chosenIndex.getKeyFieldNames().size() > 1 && indexApplicableVarFoundCount > 1) {
            requireVerificationAfterSIdxSearch = true;
        }

        // Step 2 -
        // Checks whether operators after the SELECT or JOIN operator only use PK and/or SK field variables.
        // We don't have to consider the variables produced and used after the SELECT or JOIN operator.
        indexOnlyPlanInfo.setFirst(isIndexOnlyPlan);
        indexOnlyPlanInfo.setThird(requireVerificationAfterSIdxSearch);
        indexOnlyPlanInfo.setFourth(doesSIdxSearchCoverAllPredicates);
        if (!isIndexOnlyPlan) {
            return;
        }

        checkVarUsageAfterSelectOp(afterTopRefs, liveVarsAfterSelJoinOp, dataScanPKVars, chosenIndexVars, chosenIndex,
                indexSubTree, chosenIndexFieldNames, dataScanRecordVars, context, constantVars, indexOnlyPlanInfo);
        isIndexOnlyPlan = indexOnlyPlanInfo.getFirst();
        secondaryKeyFieldUsedAfterSelectOrJoinOp = indexOnlyPlanInfo.getSecond();

        // R-Tree specific case check
        // We still need to check two more conditions if the given index is R-tree.
        if (chosenIndex.getIndexType() == IndexType.RTREE) {
            boolean rTreeCheck = checkRTreeSpecificIdxOnlyCondition(probeSubTree, indexSubTree, chosenIndex,
                    analysisCtx, matchedFuncExprs, liveVarsAfterSelJoinOp, indexOnlyPlanInfo);
            if (rTreeCheck) {
                isIndexOnlyPlan = indexOnlyPlanInfo.getFirst();
                requireVerificationAfterSIdxSearch = indexOnlyPlanInfo.getThird();
            }
        }

        indexOnlyPlanInfo.setFirst(isIndexOnlyPlan);
        indexOnlyPlanInfo.setSecond(secondaryKeyFieldUsedAfterSelectOrJoinOp);
        indexOnlyPlanInfo.setThird(requireVerificationAfterSIdxSearch);
        indexOnlyPlanInfo.setFourth(doesSIdxSearchCoverAllPredicates);
    }

    /**
     * Adds each variable from the given source list to the target list.
     *
     * @param sourceList
     * @param targetList
     */
    private static void copyVarsToAnotherList(List<LogicalVariable> sourceList, List<LogicalVariable> targetList) {
        for (LogicalVariable v : sourceList) {
            if (!targetList.contains(v)) {
                targetList.add(v);
            }
        }
    }

    private static void copyVarsToAnotherList(Set<LogicalVariable> sourceSet, List<LogicalVariable> targetList) {
        for (LogicalVariable v : sourceSet) {
            if (!targetList.contains(v)) {
                targetList.add(v);
            }
        }
    }

    /**
     * As the first step of index-only plan check, this method checks whether variables used in the given optimizable
     * expressions of the SELECT (JOIN) operator are from a secondary index and/or the primary index.
     */
    private static boolean checkVarUsageInSelectOrJoinCondExpr(List<IOptimizableFuncExpr> matchedFuncExprs,
            List<LogicalVariable> usedVarsInSelJoinOp, List<List<String>> PKfieldNames,
            List<List<String>> chosenIndexFieldNames, List<LogicalVariable> chosenIndexVars) {
        boolean exprOnlyUsesVarsFromIndex = false;
        for (IOptimizableFuncExpr matchedFuncExpr : matchedFuncExprs) {
            // for each variable that is used in the select (join) condition,
            for (LogicalVariable conditionVar : usedVarsInSelJoinOp) {
                int varIndex = matchedFuncExpr.findLogicalVar(conditionVar);
                if (varIndex == -1) {
                    // Could not find this variable in the optimizable function expression.
                    continue;
                }
                // Found this var in the optimizable function expression.
                List<String> fieldNameOfSelectVars = matchedFuncExpr.getFieldName(varIndex);

                // Does this variable come from the primary index or a secondary index?
                int keyFoundInPIdx = PKfieldNames.indexOf(fieldNameOfSelectVars);
                int keyFoundInSIdx = chosenIndexFieldNames.indexOf(fieldNameOfSelectVars);

                if (keyFoundInPIdx < 0 && keyFoundInSIdx < 0) {
                    // If this variable does not come from SK or PK, then the given plan is not an index-only plan.
                    exprOnlyUsesVarsFromIndex = false;
                    break;
                } else {
                    // This variable comes from the chosen index or PK.
                    if (!chosenIndexVars.contains(conditionVar)) {
                        chosenIndexVars.add(conditionVar);
                    }
                    exprOnlyUsesVarsFromIndex = true;
                }
            }
            if (!exprOnlyUsesVarsFromIndex) {
                // If we find one violation, then clearly this is not an index-only plan.
                // We can stop checking.
                break;
            }
        }

        return exprOnlyUsesVarsFromIndex;
    }

    /**
     * As the second step of index-only plan check, this method checks whether variables used in the given optimizable
     * expressions of the SELECT (JOIN) operator are the only variables that are used after the SELECT (JOIN) operator
     * unless the variables are produced after the SELECT (JOIN) operator.
     *
     * @return Pair<Boolean, Boolean>: the first boolean value tells whether the given plan is an index-only plan.
     *         The second boolean value tells whether the secondary key field variable(s) are used after the given
     *         SELECT (JOIN) operator.
     * @throws AlgebricksException
     *
     */
    private static void checkVarUsageAfterSelectOp(List<Mutable<ILogicalOperator>> afterSelectOpRefs,
            List<LogicalVariable> liveVarsAfterSelJoinOp, List<LogicalVariable> dataScanPKVars,
            List<LogicalVariable> chosenIndexVars, Index chosenIndex, OptimizableOperatorSubTree indexSubTree,
            List<List<String>> chosenIndexFieldNames, List<LogicalVariable> dataScanRecordVars,
            IOptimizationContext context, List<LogicalVariable> constantVars,
            Quadruple<Boolean, Boolean, Boolean, Boolean> indexOnlyPlanInfo) throws AlgebricksException {
        if (afterSelectOpRefs == null) {
            indexOnlyPlanInfo.setFirst(false);
            indexOnlyPlanInfo.setSecond(false);
            return;
        }
        boolean isIndexOnlyPlan = indexOnlyPlanInfo.getFirst();
        boolean secondaryKeyFieldUsedAfterSelectOrJoinOp = indexOnlyPlanInfo.getSecond();
        boolean countAggFunctionIsUsedInThePlan = false;
        List<LogicalVariable> usedVarsInCount = new ArrayList<>();
        List<LogicalVariable> producedVarsAfterSelectOrJoinOp = new ArrayList<>();
        List<LogicalVariable> usedVarsAfterSelectOrJoinOp = new ArrayList<>();

        AbstractLogicalOperator afterSelectRefOp;
        AggregateOperator aggOp;

        ILogicalExpression condExpr;
        List<Mutable<ILogicalExpression>> condExprs;
        AbstractFunctionCallExpression condExprFnCall;

        // Checks each operator after the given SELECT (JOIN) operator.
        for (Mutable<ILogicalOperator> afterSelectRef : afterSelectOpRefs) {
            usedVarsAfterSelectOrJoinOp.clear();
            producedVarsAfterSelectOrJoinOp.clear();
            VariableUtilities.getUsedVariables(afterSelectRef.getValue(), usedVarsAfterSelectOrJoinOp);
            VariableUtilities.getProducedVariables(afterSelectRef.getValue(), producedVarsAfterSelectOrJoinOp);
            // Checks whether COUNT exists in the given plan since we can substitute record variable
            // with the PK variable as an optimization because COUNT(record) is equal to COUNT(PK).
            // For this case only, we can replace the record variable with the PK variable.
            afterSelectRefOp = (AbstractLogicalOperator) afterSelectRef.getValue();
            if (afterSelectRefOp.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                aggOp = (AggregateOperator) afterSelectRefOp;
                condExprs = aggOp.getExpressions();
                for (int i = 0; i < condExprs.size(); i++) {
                    condExpr = condExprs.get(i).getValue();
                    if (condExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                        condExprFnCall = (AbstractFunctionCallExpression) condExpr;
                        if (condExprFnCall.getFunctionIdentifier() == BuiltinFunctions.COUNT) {
                            // COUNT found. count on the record ($$0) can be replaced as the PK variable.
                            countAggFunctionIsUsedInThePlan = true;
                            VariableUtilities.getUsedVariables(afterSelectRef.getValue(), usedVarsInCount);
                            break;
                        }
                    }
                }
            }

            for (LogicalVariable usedVarAfterSelectOrJoinOp : usedVarsAfterSelectOrJoinOp) {
                // This variable should be live in the SELECT (JOIN) operator.
                // If not, a check is not necessary since this variable is produced after the SELECT operator.
                if (!liveVarsAfterSelJoinOp.contains(usedVarAfterSelectOrJoinOp)) {
                    continue;
                }
                // From PK?
                if (dataScanPKVars.contains(usedVarAfterSelectOrJoinOp)) {
                    isIndexOnlyPlan = true;
                    continue;
                }
                // From SK?
                if (chosenIndexVars.contains(usedVarAfterSelectOrJoinOp)) {
                    secondaryKeyFieldUsedAfterSelectOrJoinOp = true;
                    if (chosenIndex.getIndexType() == IndexType.BTREE
                            || chosenIndex.getIndexType() == IndexType.RTREE) {
                        isIndexOnlyPlan = true;
                        continue;
                    } else {
                        // Inverted Index Case:
                        // Unlike B+Tree or R-Tree on a POINT or a RECTANGLE type, we can't use
                        // or reconstruct secondary key field value from the SK value since a SK
                        // is just part of a field value.
                        // Therefore, if a secondary key field value is used after SELECT (JOIN)
                        // operator, this cannot be an index-only plan.
                        // Therefore, we can only check whether PK is used after SELECT operator.
                        isIndexOnlyPlan = false;
                        break;
                    }
                }
                // If an ASSIGN or UNNEST before the given SELECT (JOIN) operator contains
                // the given variable and the given variable is a secondary key field
                // (this happens when we have a composite secondary index)
                if (indexSubTree.getVarsToFieldNameMap().containsKey(usedVarAfterSelectOrJoinOp)) {
                    if (chosenIndexFieldNames
                            .contains(indexSubTree.getVarsToFieldNameMap().get(usedVarAfterSelectOrJoinOp))) {
                        isIndexOnlyPlan = true;
                        secondaryKeyFieldUsedAfterSelectOrJoinOp = true;
                        continue;
                    } else {
                        // Non-PK or non-secondary key field is used after SELECT operator.
                        // This is not an index-only plan.
                        isIndexOnlyPlan = false;
                        break;
                    }
                }
                // The only case that we allow is when a record variable is used
                // with count either directly or indirectly via a record-constructor.
                if (dataScanRecordVars.contains(usedVarAfterSelectOrJoinOp)) {
                    if (!countAggFunctionIsUsedInThePlan) {
                        // We don't need to care about this case since COUNT is not used.
                        isIndexOnlyPlan = false;
                        break;
                    } else if (usedVarsInCount.contains(usedVarAfterSelectOrJoinOp)
                            || usedVarsInCount.containsAll(producedVarsAfterSelectOrJoinOp)) {
                        VariableUtilities.substituteVariables(afterSelectRefOp, usedVarAfterSelectOrJoinOp,
                                dataScanPKVars.get(0), context);
                        isIndexOnlyPlan = true;
                        continue;
                    } else {
                        isIndexOnlyPlan = false;
                        break;
                    }
                }
                // If this variable contains a constant expression,
                // this does not affect our check and we can continue.
                if (constantVars.contains(usedVarAfterSelectOrJoinOp)) {
                    isIndexOnlyPlan = true;
                    continue;
                } else {
                    isIndexOnlyPlan = false;
                    break;
                }
            }
            if (!isIndexOnlyPlan) {
                break;
            }
        }

        indexOnlyPlanInfo.setFirst(isIndexOnlyPlan);
        indexOnlyPlanInfo.setSecond(secondaryKeyFieldUsedAfterSelectOrJoinOp);
    }

    /**
     * For R-Tree only check condition:
     * This method assumes that we already know that the given plan is an index-only plan.
     * We have one more condition for R-Tree index:
     * Condition 1: The key field type of the index should be either point or rectangle.
     * The above condition must hold since we can't reconstruct the original field value otherwise.
     * If this is the case and the following condition is met, we don't need to
     * do a post-processing. That is, the given index will not generate a superset of the final results.
     * If not, we need to add a "SELECT" operator to the path where instantTryLock on PK succeeds, too.
     * Condition 2: Query shape should be point or rectangle.
     */
    private static boolean checkRTreeSpecificIdxOnlyCondition(OptimizableOperatorSubTree probeSubTree,
            OptimizableOperatorSubTree indexSubTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            List<IOptimizableFuncExpr> matchedFuncExprs, List<LogicalVariable> liveVarsInSelJoinOp,
            Quadruple<Boolean, Boolean, Boolean, Boolean> indexOnlyPlanInfo) throws AlgebricksException {

        boolean isIndexOnlyPlan = indexOnlyPlanInfo.getFirst();
        boolean requireVerificationAfterSIdxSearch = indexOnlyPlanInfo.getThird();

        ILogicalExpression condExpr;
        AbstractFunctionCallExpression condExprFnCall;

        // TODO: We can probably do something smarter here based on selectivity or MBR area.
        IOptimizableFuncExpr optFuncExpr = AccessMethodUtils.chooseFirstOptFuncExpr(chosenIndex, analysisCtx);
        ARecordType recordType = indexSubTree.getRecordType();
        ARecordType probeRecordType = null;

        if (probeSubTree != null) {
            probeRecordType = probeSubTree.getRecordType();
        }

        int optFieldIdx = AccessMethodUtils.chooseFirstOptFuncVar(chosenIndex, analysisCtx);
        Pair<IAType, Boolean> keyPairType = Index.getNonNullableOpenFieldType(optFuncExpr.getFieldType(optFieldIdx),
                optFuncExpr.getFieldName(optFieldIdx), recordType);
        if (keyPairType == null) {
            return false;
        }

        if (matchedFuncExprs.size() == 1) {
            condExpr = optFuncExpr.getFuncExpr();
            condExprFnCall = (AbstractFunctionCallExpression) condExpr;
            for (int i = 0; i < condExprFnCall.getArguments().size(); i++) {
                Mutable<ILogicalExpression> expr = condExprFnCall.getArguments().get(i);
                // For SELECT case, we check whether an index is on POINT or RECTANGLE index.
                if (expr.getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                    AsterixConstantValue tmpVal =
                            (AsterixConstantValue) ((ConstantExpression) expr.getValue()).getValue();
                    // SELECT condition
                    if (tmpVal.getObject().getType().getTypeTag() == ATypeTag.POINT
                            || tmpVal.getObject().getType().getTypeTag() == ATypeTag.RECTANGLE) {
                        // Index type
                        if (keyPairType.first.getTypeTag() == ATypeTag.POINT
                                || keyPairType.first.getTypeTag() == ATypeTag.RECTANGLE) {
                            requireVerificationAfterSIdxSearch = false;
                        } else {
                            requireVerificationAfterSIdxSearch = true;
                            break;
                        }
                    } else {
                        requireVerificationAfterSIdxSearch = true;
                        break;
                    }
                } else if (expr.getValue().getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                    // We are dealing with a JOIN case here.
                    LogicalVariable tmpVal = ((VariableReferenceExpression) expr.getValue()).getVariableReference();

                    // We only need to take care of the variables from the probe tree here.
                    // liveVarsInSelJoinOp only contains live variables in the index sub tree.
                    // Since we know the type of the given index from index sub tree,
                    // we need to find the type of other join variable.
                    if (liveVarsInSelJoinOp.contains(tmpVal)) {
                        continue;
                    }

                    List<String> tmpValFieldName;
                    IAType tmpValFieldType = null;

                    ILogicalExpression tmpCondExpr;
                    AbstractFunctionCallExpression tmpCondExprCall;
                    FunctionIdentifier tmpFuncID;

                    if (probeSubTree != null) {
                        // We first check whether the given variable is produced from an ASSIGN.
                        for (int j = 0; j < probeSubTree.getAssignsAndUnnestsRefs().size(); j++) {
                            List<LogicalVariable> producedVarsFromProbeTree = new ArrayList<>();
                            ILogicalOperator tmpOp = probeSubTree.getAssignsAndUnnestsRefs().get(j).getValue();
                            VariableUtilities.getProducedVariables(tmpOp, producedVarsFromProbeTree);

                            // If this is the assign (unnest-map) that we are looking for:
                            if (producedVarsFromProbeTree.contains(tmpVal)) {
                                // If the operator is ASSIGN
                                if (tmpOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                                    AssignOperator tmpAssignOp = (AssignOperator) tmpOp;
                                    List<Mutable<ILogicalExpression>> tmpCondExprs = tmpAssignOp.getExpressions();
                                    for (Mutable<ILogicalExpression> tmpConditionExpr : tmpCondExprs) {
                                        if (tmpConditionExpr.getValue()
                                                .getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                                            tmpCondExpr = tmpConditionExpr.getValue();
                                            tmpCondExprCall = (AbstractFunctionCallExpression) tmpCondExpr;
                                            tmpFuncID = tmpCondExprCall.getFunctionIdentifier();
                                            // Get the field type for the given variable
                                            tmpValFieldType = findSpatialType(tmpFuncID);
                                            if (tmpValFieldType == null) {
                                                continue;
                                            } else {
                                                break;
                                            }
                                        }
                                    }
                                } else if (tmpOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
                                    // If the operator is UNNESTMAP
                                    UnnestMapOperator tmpUnnestMapOp = (UnnestMapOperator) tmpOp;
                                    tmpCondExpr = tmpUnnestMapOp.getExpressionRef().getValue();
                                    tmpCondExprCall = (AbstractFunctionCallExpression) tmpCondExpr;
                                    tmpFuncID = tmpCondExprCall.getFunctionIdentifier();
                                    tmpValFieldType = findSpatialType(tmpFuncID);
                                    if (tmpValFieldType == null) {
                                        continue;
                                    } else {
                                        break;
                                    }
                                } else {
                                    // We only care ASSIGN or UNNEST_MAP operator.
                                    continue;
                                }
                            }
                        }
                    }

                    // We tried to find the field type of the given variable, but couldn't.
                    // This variable is a direct field from the probe tree so that we can find the field type.
                    if (tmpValFieldType == null) {
                        tmpValFieldName = probeSubTree.getVarsToFieldNameMap().get(tmpVal);

                        if (tmpValFieldName != null) {
                            for (int j = 0; j < probeRecordType.getFieldNames().length; j++) {
                                String fieldName = probeRecordType.getFieldNames()[j];
                                if (tmpValFieldName.contains(fieldName)) {
                                    tmpValFieldType = probeRecordType.getFieldType(fieldName);
                                    break;
                                }
                            }
                        }
                    }

                    if (keyPairType.first.getTypeTag() == ATypeTag.POINT
                            || keyPairType.first.getTypeTag() == ATypeTag.RECTANGLE) {
                        // If the given field from the other join branch is a POINT or a RECTANGLE,
                        // we don't need to verify it again using SELECT operator since there will be
                        // no false positive results.
                        if (tmpValFieldType != null && (tmpValFieldType.getTypeTag() == ATypeTag.POINT
                                || tmpValFieldType.getTypeTag() == ATypeTag.RECTANGLE)) {
                            requireVerificationAfterSIdxSearch = false;
                        } else {
                            requireVerificationAfterSIdxSearch = true;
                        }
                    } else {
                        // If the type of an R-Tree index is not on a point or rectangle field,
                        // an index-only plan is not possible since we can't reconstruct
                        // the original field value from an R-Tree index search.
                        isIndexOnlyPlan = false;
                        requireVerificationAfterSIdxSearch = true;
                    }
                }
            }
        } else {
            requireVerificationAfterSIdxSearch = true;
        }

        indexOnlyPlanInfo.setFirst(isIndexOnlyPlan);
        indexOnlyPlanInfo.setThird(requireVerificationAfterSIdxSearch);

        return true;
    }

    /**
     * Checks whether the given index is an inverted index or not.
     */
    public static boolean isInvertedIndex(Index index) {
        switch (index.getIndexType()) {
            case SINGLE_PARTITION_NGRAM_INVIX:
            case SINGLE_PARTITION_WORD_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
                return true;
            default:
                return false;
        }
    }

    /**
     * Finds a corresponding IAType for the given function identifier.
     */
    public static IAType findSpatialType(FunctionIdentifier fid) {
        if (fid == BuiltinFunctions.CREATE_CIRCLE || fid == BuiltinFunctions.CIRCLE_CONSTRUCTOR) {
            return BuiltinType.ACIRCLE;
        } else if (fid == BuiltinFunctions.CREATE_POINT || fid == BuiltinFunctions.POINT_CONSTRUCTOR) {
            return BuiltinType.APOINT;
        } else if (fid == BuiltinFunctions.CREATE_RECTANGLE || fid == BuiltinFunctions.RECTANGLE_CONSTRUCTOR) {
            return BuiltinType.ARECTANGLE;
        } else if (fid == BuiltinFunctions.CREATE_POLYGON || fid == BuiltinFunctions.POLYGON_CONSTRUCTOR) {
            return BuiltinType.APOLYGON;
        } else if (fid == BuiltinFunctions.CREATE_LINE || fid == BuiltinFunctions.LINE_CONSTRUCTOR) {
            return BuiltinType.ALINE;
        } else {
            return null;
        }
    }

    /**
     * Fetches variables that contains constant expressions.
     *
     * @param assignOps
     * @param targetVars
     */
    private static void getConstantVars(List<AbstractLogicalOperator> assignOps, List<LogicalVariable> targetVars) {
        ILogicalExpression condExpr;
        List<Mutable<ILogicalExpression>> condExprs;

        for (AbstractLogicalOperator assignUnnestOp : assignOps) {
            if (assignUnnestOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                AssignOperator assignOp = (AssignOperator) assignUnnestOp;
                condExprs = assignOp.getExpressions();
                for (int i = 0; i < condExprs.size(); i++) {
                    condExpr = condExprs.get(i).getValue();
                    if (condExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT
                            && !targetVars.contains(assignOp.getVariables().get(i))) {
                        targetVars.add(assignOp.getVariables().get(i));
                    }
                }
            }
        }
    }

    /**
     * Finds the datasource from an index-utilization plan for the following types of the plan:
     * a. index-only plan
     * b. non-index-only plan
     */
    public static ILogicalOperator findDataSourceFromIndexUtilizationPlan(ILogicalOperator topOp) {
        if (topOp == null) {
            return null;
        }

        ILogicalOperator dataSourceOp = topOp;

        // Non-index-only plan case: UNNEST_MAP or LEFT_OUTER_UNNEST_MAP is placed in the top level.
        switch (topOp.getOperatorTag()) {
            case UNNEST_MAP:
            case LEFT_OUTER_UNNEST_MAP:
                return topOp;
            case UNIONALL:
                dataSourceOp = dataSourceOp.getInputs().get(0).getValue();
                // Index-only plan case:
                // The order of operators: 7 unionall <- 6 select <- 5 assign?
                // <- 4 unnest-map (PIdx) <- 3 split <- 2 unnest-map (SIdx) <- ...
                // We do this to skip the primary index-search since we are looking for a secondary index-search here.
                do {
                    dataSourceOp = dataSourceOp.getInputs().get(0).getValue();
                } while (dataSourceOp.getOperatorTag() != LogicalOperatorTag.SPLIT && dataSourceOp.hasInputs());

                if (dataSourceOp.getOperatorTag() != LogicalOperatorTag.SPLIT) {
                    // The current operator should be SPLIT. Otherwise, this is not an index-only plan.
                    return null;
                }

                do {
                    dataSourceOp = dataSourceOp.getInputs().get(0).getValue();
                } while (dataSourceOp.getOperatorTag() != LogicalOperatorTag.UNNEST_MAP
                        && dataSourceOp.getOperatorTag() != LogicalOperatorTag.LEFT_OUTER_UNNEST_MAP
                        && dataSourceOp.hasInputs());

                // Should be unnest-map now
                if (dataSourceOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP
                        || dataSourceOp.getOperatorTag() == LogicalOperatorTag.LEFT_OUTER_UNNEST_MAP) {
                    return dataSourceOp;
                } else {
                    // Otherwise, this is not an index-only plan. So, returns null.
                    return null;
                }
            default:
                return null;
        }

    }

    /**
     * Resets the variable mapping in an UNIONALL Operator in an index-only plan
     * in case of group-by Missing expression in Left-Outer-Join (LOJ).
     *
     * @throws AlgebricksException
     */
    public static ILogicalOperator resetVariableMappingInUnionOpInIndexOnlyPlan(boolean LOJVarExist,
            List<LogicalVariable> LOJMissingVariables, ILogicalOperator unionAllOp,
            List<Mutable<ILogicalOperator>> aboveTopRefs, IOptimizationContext context) throws AlgebricksException {
        // For an index-only plan, if newNullPlaceHolderVar is not in the variable map of the UNIONALL operator,
        // we need to add this variable to the map.
        // Also, we need to delete replaced variables in the map if it was used only in the group-by operator.
        if (unionAllOp.getOperatorTag() != LogicalOperatorTag.UNIONALL) {
            return unionAllOp;
        }

        // First, check whether the given old variable can be deleted. If it is used somewhere else
        // except the group-by operator, we can't delete it since we need to propagate it.
        boolean LOJVarCanBeDeleted = true;
        if (LOJVarExist) {
            List<LogicalVariable> usedVars = new ArrayList<>();
            for (int i = 0; i < aboveTopRefs.size(); i++) {
                usedVars.clear();
                ILogicalOperator lOp = aboveTopRefs.get(i).getValue();
                VariableUtilities.getUsedVariables(lOp, usedVars);
                if (usedVars.containsAll(LOJMissingVariables) && lOp.getOperatorTag() != LogicalOperatorTag.GROUP) {
                    LOJVarCanBeDeleted = false;
                    break;
                }
            }
        }

        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varMap =
                ((UnionAllOperator) unionAllOp).getVariableMappings();

        if (LOJVarExist && LOJVarCanBeDeleted) {
            // Delete old variables from the map.
            for (Iterator<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> it = varMap.iterator(); it
                    .hasNext();) {
                Triple<LogicalVariable, LogicalVariable, LogicalVariable> tripleVars = it.next();
                if (tripleVars.first.equals(LOJMissingVariables.get(0))
                        || tripleVars.second.equals(LOJMissingVariables.get(0))
                        || tripleVars.third.equals(LOJMissingVariables.get(0))) {
                    it.remove();
                }
            }
        }

        if (LOJVarExist && LOJVarCanBeDeleted) {
            UnionAllOperator newUnionAllOp = new UnionAllOperator(varMap);
            newUnionAllOp.getInputs()
                    .add(new MutableObject<ILogicalOperator>(unionAllOp.getInputs().get(0).getValue()));
            newUnionAllOp.getInputs()
                    .add(new MutableObject<ILogicalOperator>(unionAllOp.getInputs().get(1).getValue()));
            context.computeAndSetTypeEnvironmentForOperator(newUnionAllOp);
            return newUnionAllOp;
        } else {
            return unionAllOp;
        }
    }

    /**
     * Checks whether a LogicalVariable exists in a list of Triple<LogicalVariable, LogicalVariable, LogicalVariable>.
     *
     * @param varsList
     *            list that contains triples of LogicalVariable.
     * @param varToFind
     *            a LogicalVariable to find
     * @param checkOnlyFirst
     *            specifies whether it is required to check only the first variable in the given triple.
     * @return
     */
    public static boolean findVarInTripleVarList(
            List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varsList, LogicalVariable varToFind,
            boolean checkOnlyFirst) {
        for (Iterator<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> it = varsList.iterator(); it
                .hasNext();) {
            Triple<LogicalVariable, LogicalVariable, LogicalVariable> itVars = it.next();
            if (varToFind == itVars.first) {
                return true;
            }
            if (!checkOnlyFirst) {
                if (varToFind == itVars.second || varToFind == itVars.third) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Gets the specified no-index-only option in a query.
     *
     * @param context
     * @return true if no-index-only plan is true.
     *         false otherwise.
     */
    public static boolean getNoIndexOnlyOption(IOptimizationContext context) {
        Map<String, Object> config = context.getMetadataProvider().getConfig();
        if (config.containsKey(AbstractIntroduceAccessMethodRule.NO_INDEX_ONLY_PLAN_OPTION)) {
            return Boolean
                    .parseBoolean((String) config.get(AbstractIntroduceAccessMethodRule.NO_INDEX_ONLY_PLAN_OPTION));
        }
        return AbstractIntroduceAccessMethodRule.NO_INDEX_ONLY_PLAN_OPTION_DEFAULT_VALUE;
    }

}
