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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.dataflow.data.common.ExpressionTypeComputer;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.optimizer.base.AnalysisUtil;
import org.apache.asterix.optimizer.rules.am.OptimizableOperatorSubTree.DataSourceType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

import com.google.common.collect.ImmutableSet;

/**
 * Class that embodies the commonalities between rewrite rules for access
 * methods.
 */
public abstract class AbstractIntroduceAccessMethodRule implements IAlgebraicRewriteRule {
    // When this option is set to true before executing a query, we don't apply the index-only plan.
    public final static String NO_INDEX_ONLY_PLAN_OPTION = "noindexonly";
    public final static boolean NO_INDEX_ONLY_PLAN_OPTION_DEFAULT_VALUE = false;

    protected MetadataProvider metadataProvider;

    // Function Identifier sets that retain the original field variable through each function's arguments
    private final ImmutableSet<FunctionIdentifier> funcIDSetThatRetainFieldName =
            ImmutableSet.of(BuiltinFunctions.WORD_TOKENS, BuiltinFunctions.GRAM_TOKENS, BuiltinFunctions.SUBSTRING,
                    BuiltinFunctions.SUBSTRING_BEFORE, BuiltinFunctions.SUBSTRING_AFTER,
                    BuiltinFunctions.CREATE_POLYGON, BuiltinFunctions.CREATE_MBR, BuiltinFunctions.CREATE_RECTANGLE,
                    BuiltinFunctions.CREATE_CIRCLE, BuiltinFunctions.CREATE_LINE, BuiltinFunctions.CREATE_POINT,
                    BuiltinFunctions.NUMERIC_ADD, BuiltinFunctions.NUMERIC_SUBTRACT, BuiltinFunctions.NUMERIC_MULTIPLY,
                    BuiltinFunctions.NUMERIC_DIVIDE, BuiltinFunctions.NUMERIC_DIV, BuiltinFunctions.NUMERIC_MOD);

    public abstract Map<FunctionIdentifier, List<IAccessMethod>> getAccessMethods();

    protected static void registerAccessMethod(IAccessMethod accessMethod,
            Map<FunctionIdentifier, List<IAccessMethod>> accessMethods) {
        List<Pair<FunctionIdentifier, Boolean>> funcs = accessMethod.getOptimizableFunctions();
        for (Pair<FunctionIdentifier, Boolean> funcIdent : funcs) {
            List<IAccessMethod> l = accessMethods.get(funcIdent.first);
            if (l == null) {
                l = new ArrayList<IAccessMethod>();
                accessMethods.put(funcIdent.first, l);
            }
            l.add(accessMethod);
        }
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    protected void setMetadataDeclarations(IOptimizationContext context) {
        metadataProvider = (MetadataProvider) context.getMetadataProvider();
    }

    protected void fillSubTreeIndexExprs(OptimizableOperatorSubTree subTree,
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs, IOptimizationContext context)
            throws AlgebricksException {
        fillSubTreeIndexExprs(subTree, analyzedAMs, context, false);
    }

    /**
     * Fills the information about the given optimizable function expressions using the subtree.
     *
     * @param subTree
     * @param analyzedAMs
     * @param context
     * @param isArbitraryFormOfSubtree
     *            if the given subtree is in an arbitrary form that OptimizableSubTree class can't initialize, we try
     *            to fill the field type of each variable that is used in the optimizable function expressions.
     *            This way, an index-nested-loop-join transformation can be conducted properly since the transformation
     *            process skips an optimzable function expression if the field type of one of its variable is unknown.
     * @throws AlgebricksException
     */
    protected void fillSubTreeIndexExprs(OptimizableOperatorSubTree subTree,
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs, IOptimizationContext context,
            boolean isArbitraryFormOfSubtree) throws AlgebricksException {
        Iterator<Map.Entry<IAccessMethod, AccessMethodAnalysisContext>> amIt = analyzedAMs.entrySet().iterator();
        // Check applicability of indexes by access method type.
        while (amIt.hasNext()) {
            Map.Entry<IAccessMethod, AccessMethodAnalysisContext> entry = amIt.next();
            AccessMethodAnalysisContext amCtx = entry.getValue();
            // For the current access method type, map variables to applicable
            // indexes.
            if (!isArbitraryFormOfSubtree) {
                fillAllIndexExprs(subTree, amCtx, context);
            } else {
                fillVarFieldTypeForOptFuncExprs(subTree, amCtx, context);
            }
        }
    }

    /**
     * Sets the subtree and the field type for the variables in the given function expression.
     * This method is only used for an arbitrary form of the probe-subtree in a join.
     */
    protected void fillVarFieldTypeForOptFuncExprs(OptimizableOperatorSubTree subTree,
            AccessMethodAnalysisContext analysisCtx, ITypingContext context) throws AlgebricksException {
        ILogicalOperator rootOp = subTree.getRoot();
        IVariableTypeEnvironment envSubtree = context.getOutputTypeEnvironment(rootOp);
        Set<LogicalVariable> liveVarsAtRootOp = new HashSet<LogicalVariable>();
        VariableUtilities.getLiveVariables(rootOp, liveVarsAtRootOp);

        // For each optimizable function expression, applies the field type of each variable.
        for (IOptimizableFuncExpr optFuncExpr : analysisCtx.getMatchedFuncExprs()) {
            for (LogicalVariable var : liveVarsAtRootOp) {
                int optVarIndex = optFuncExpr.findLogicalVar(var);
                if (optVarIndex < 0) {
                    continue;
                }
                optFuncExpr.setOptimizableSubTree(optVarIndex, subTree);
                IAType fieldType = (IAType) envSubtree.getVarType(var);
                optFuncExpr.setFieldType(optVarIndex, fieldType);
            }
        }
    }

    protected void pruneIndexCandidates(Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs,
            IOptimizationContext context, IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {
        Iterator<Map.Entry<IAccessMethod, AccessMethodAnalysisContext>> amIt = analyzedAMs.entrySet().iterator();
        // Check applicability of indexes by access method type.
        while (amIt.hasNext()) {
            Map.Entry<IAccessMethod, AccessMethodAnalysisContext> entry = amIt.next();
            AccessMethodAnalysisContext amCtx = entry.getValue();
            pruneIndexCandidates(entry.getKey(), amCtx, context, typeEnvironment);
            // Remove access methods for which there are definitely no
            // applicable indexes.
            if (amCtx.isIndexExprsAndVarsEmpty()) {
                amIt.remove();
            }
        }
    }

    /**
     * Simply picks the first index that it finds. TODO: Improve this decision
     * process by making it more systematic.
     */
    protected Pair<IAccessMethod, Index> chooseBestIndex(Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs) {
        List<Pair<IAccessMethod, Index>> list = chooseAllIndexes(analyzedAMs);
        return list.isEmpty() ? null : list.get(0);
    }

    /**
     * Choose all indexes that match the given access method. These indexes will be used as index-search
     * to replace the given predicates in a SELECT operator. Also, if there are multiple same type of indexes
     * on the same field, only of them will be chosen. Allowed cases (AccessMethod, IndexType) are:
     * [BTreeAccessMethod , IndexType.BTREE], [RTreeAccessMethod , IndexType.RTREE],
     * [InvertedIndexAccessMethod, IndexType.SINGLE_PARTITION_WORD_INVIX || SINGLE_PARTITION_NGRAM_INVIX ||
     * LENGTH_PARTITIONED_WORD_INVIX || LENGTH_PARTITIONED_NGRAM_INVIX]
     */
    protected List<Pair<IAccessMethod, Index>> chooseAllIndexes(
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs) {
        List<Pair<IAccessMethod, Index>> result = new ArrayList<>();
        // Use variables (fields) to the index types map to check which type of indexes are applied for the vars.
        Map<List<Pair<Integer, Integer>>, List<IndexType>> resultVarsToIndexTypesMap = new HashMap<>();
        Iterator<Map.Entry<IAccessMethod, AccessMethodAnalysisContext>> amIt = analyzedAMs.entrySet().iterator();
        while (amIt.hasNext()) {
            Map.Entry<IAccessMethod, AccessMethodAnalysisContext> amEntry = amIt.next();
            AccessMethodAnalysisContext analysisCtx = amEntry.getValue();
            Iterator<Map.Entry<Index, List<Pair<Integer, Integer>>>> indexIt =
                    analysisCtx.getIteratorForIndexExprsAndVars();
            while (indexIt.hasNext()) {
                Map.Entry<Index, List<Pair<Integer, Integer>>> indexEntry = indexIt.next();
                IAccessMethod chosenAccessMethod = amEntry.getKey();
                Index chosenIndex = indexEntry.getKey();
                IndexType indexType = chosenIndex.getIndexType();
                boolean isKeywordOrNgramIndexChosen = indexType == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                        || indexType == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX
                        || indexType == IndexType.SINGLE_PARTITION_WORD_INVIX
                        || indexType == IndexType.SINGLE_PARTITION_NGRAM_INVIX;
                if ((chosenAccessMethod == BTreeAccessMethod.INSTANCE && indexType == IndexType.BTREE)
                        || (chosenAccessMethod == RTreeAccessMethod.INSTANCE && indexType == IndexType.RTREE)
                        || (chosenAccessMethod == InvertedIndexAccessMethod.INSTANCE && isKeywordOrNgramIndexChosen)) {
                    if (resultVarsToIndexTypesMap.containsKey(indexEntry.getValue())) {
                        List<IndexType> appliedIndexTypes = resultVarsToIndexTypesMap.get(indexEntry.getValue());
                        if (!appliedIndexTypes.contains(indexType)) {
                            appliedIndexTypes.add(indexType);
                            result.add(new Pair<>(chosenAccessMethod, chosenIndex));
                        }
                    } else {
                        List<IndexType> addedIndexTypes = new ArrayList<>();
                        addedIndexTypes.add(indexType);
                        resultVarsToIndexTypesMap.put(indexEntry.getValue(), addedIndexTypes);
                        result.add(new Pair<>(chosenAccessMethod, chosenIndex));
                    }
                }
            }
        }
        return result;
    }

    /**
     * Removes irrelevant access methods candidates, based on whether the
     * expressions in the query match those in the index. For example, some
     * index may require all its expressions to be matched, and some indexes may
     * only require a match on a prefix of fields to be applicable. This methods
     * removes all index candidates indexExprs that are definitely not
     * applicable according to the expressions involved.
     *
     * @throws AlgebricksException
     */
    public void pruneIndexCandidates(IAccessMethod accessMethod, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {
        Iterator<Map.Entry<Index, List<Pair<Integer, Integer>>>> indexExprAndVarIt =
                analysisCtx.getIteratorForIndexExprsAndVars();
        // Used to keep track of matched expressions (added for prefix search)
        int numMatchedKeys = 0;
        ArrayList<Integer> matchedExpressions = new ArrayList<>();
        while (indexExprAndVarIt.hasNext()) {
            Map.Entry<Index, List<Pair<Integer, Integer>>> indexExprAndVarEntry = indexExprAndVarIt.next();
            Index index = indexExprAndVarEntry.getKey();

            boolean allUsed = true;
            int lastFieldMatched = -1;
            boolean foundKeyField = false;
            matchedExpressions.clear();
            numMatchedKeys = 0;

            for (int i = 0; i < index.getKeyFieldNames().size(); i++) {
                List<String> keyField = index.getKeyFieldNames().get(i);
                final IAType keyType = index.getKeyFieldTypes().get(i);
                Iterator<Pair<Integer, Integer>> exprsAndVarIter = indexExprAndVarEntry.getValue().iterator();
                while (exprsAndVarIter.hasNext()) {
                    final Pair<Integer, Integer> exprAndVarIdx = exprsAndVarIter.next();
                    final IOptimizableFuncExpr optFuncExpr = analysisCtx.getMatchedFuncExpr(exprAndVarIdx.first);
                    // If expr is not optimizable by concrete index then remove
                    // expr and continue.
                    if (!accessMethod.exprIsOptimizable(index, optFuncExpr)) {
                        exprsAndVarIter.remove();
                        continue;
                    }
                    boolean typeMatch = true;
                    //Prune indexes based on field types
                    List<IAType> matchedTypes = new ArrayList<>();
                    //retrieve types of expressions joined/selected with an indexed field
                    for (int j = 0; j < optFuncExpr.getNumLogicalVars(); j++) {
                        if (j != exprAndVarIdx.second) {
                            matchedTypes.add(optFuncExpr.getFieldType(j));
                        }

                    }

                    if (matchedTypes.size() < 2 && optFuncExpr.getNumLogicalVars() == 1) {
                        matchedTypes
                                .add((IAType) ExpressionTypeComputer.INSTANCE.getType(optFuncExpr.getConstantExpr(0),
                                        context.getMetadataProvider(), typeEnvironment));
                    }

                    //infer type of logicalExpr based on index keyType
                    matchedTypes.add((IAType) ExpressionTypeComputer.INSTANCE.getType(
                            optFuncExpr.getLogicalExpr(exprAndVarIdx.second), null, new IVariableTypeEnvironment() {

                                @Override
                                public Object getVarType(LogicalVariable var) throws AlgebricksException {
                                    if (var.equals(optFuncExpr.getSourceVar(exprAndVarIdx.second))) {
                                        return keyType;
                                    }
                                    throw new IllegalArgumentException();
                                }

                                @Override
                                public Object getVarType(LogicalVariable var, List<LogicalVariable> nonNullVariables,
                                        List<List<LogicalVariable>> correlatedNullableVariableLists)
                                        throws AlgebricksException {
                                    if (var.equals(optFuncExpr.getSourceVar(exprAndVarIdx.second))) {
                                        return keyType;
                                    }
                                    throw new IllegalArgumentException();
                                }

                                @Override
                                public void setVarType(LogicalVariable var, Object type) {
                                    throw new IllegalArgumentException();
                                }

                                @Override
                                public Object getType(ILogicalExpression expr) throws AlgebricksException {
                                    return ExpressionTypeComputer.INSTANCE.getType(expr, null, this);
                                }

                                @Override
                                public boolean substituteProducedVariable(LogicalVariable v1, LogicalVariable v2)
                                        throws AlgebricksException {
                                    throw new IllegalArgumentException();
                                }
                            }));

                    //for the case when jaccard similarity is measured between ordered & unordered lists
                    boolean jaccardSimilarity = optFuncExpr.getFuncExpr().getFunctionIdentifier().getName()
                            .startsWith("similarity-jaccard-check");

                    // Full-text search consideration: an (un)ordered list of string type can be compatible with string
                    // type. i.e. an (un)ordered list can be provided as arguments to a string type field index.
                    List<IAType> elementTypes = matchedTypes;
                    if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == BuiltinFunctions.FULLTEXT_CONTAINS
                            || optFuncExpr.getFuncExpr()
                                    .getFunctionIdentifier() == BuiltinFunctions.FULLTEXT_CONTAINS_WO_OPTION) {
                        for (int j = 0; j < matchedTypes.size(); j++) {
                            if (matchedTypes.get(j).getTypeTag() == ATypeTag.ARRAY
                                    || matchedTypes.get(j).getTypeTag() == ATypeTag.MULTISET) {
                                elementTypes.set(j, ((AbstractCollectionType) matchedTypes.get(j)).getItemType());
                            }
                        }
                    }

                    for (int j = 0; j < matchedTypes.size(); j++) {
                        for (int k = j + 1; k < matchedTypes.size(); k++) {
                            typeMatch &= isMatched(elementTypes.get(j), elementTypes.get(k), jaccardSimilarity);
                        }
                    }

                    // Check if any field name in the optFuncExpr matches.
                    if (optFuncExpr.findFieldName(keyField) != -1) {
                        foundKeyField =
                                typeMatch && optFuncExpr.getOperatorSubTree(exprAndVarIdx.second).hasDataSourceScan();
                        if (foundKeyField) {
                            matchedExpressions.add(exprAndVarIdx.first);
                            numMatchedKeys++;
                            if (lastFieldMatched == i - 1) {
                                lastFieldMatched = i;
                            }
                            break;
                        }
                    }
                }
                if (!foundKeyField) {
                    allUsed = false;
                    // if any expression was matched, remove the non-matched expressions, otherwise the index is unusable
                    if (lastFieldMatched >= 0) {
                        exprsAndVarIter = indexExprAndVarEntry.getValue().iterator();
                        while (exprsAndVarIter.hasNext()) {
                            if (!matchedExpressions.contains(exprsAndVarIter.next().first)) {
                                exprsAndVarIter.remove();
                            }
                        }
                    }
                    break;
                }
            }
            // If the access method requires all exprs to be matched but they
            // are not, remove this candidate.
            if (!allUsed && accessMethod.matchAllIndexExprs()) {
                indexExprAndVarIt.remove();
                continue;
            }
            // A prefix of the index exprs may have been matched.
            if (accessMethod.matchPrefixIndexExprs()) {
                if (lastFieldMatched < 0) {
                    indexExprAndVarIt.remove();
                    continue;
                }
            }
            analysisCtx.putNumberOfMatchedKeys(index, Integer.valueOf(numMatchedKeys));
        }
    }

    private boolean isMatched(IAType type1, IAType type2, boolean useListDomain) throws AlgebricksException {
        // Sanity check - two types can't be NULL in order to be matched.
        if (type1 == null || type2 == null) {
            return false;
        }
        if (ATypeHierarchy.isSameTypeDomain(Index.getNonNullableType(type1).first.getTypeTag(),
                Index.getNonNullableType(type2).first.getTypeTag(), useListDomain)) {
            return true;
        }
        return ATypeHierarchy.canPromote(Index.getNonNullableType(type1).first.getTypeTag(),
                Index.getNonNullableType(type2).first.getTypeTag());
    }

    /**
     * Analyzes the given selection condition, filling analyzedAMs with
     * applicable access method types. At this point we are not yet consulting
     * the metadata whether an actual index exists or not.
     *
     * @throws AlgebricksException
     */
    protected boolean analyzeSelectOrJoinOpConditionAndUpdateAnalyzedAM(ILogicalExpression cond,
            List<AbstractLogicalOperator> assignsAndUnnests,
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs, IOptimizationContext context,
            IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) cond;
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();
        // TODO: We don't consider a disjunctive condition with an index yet since it's complex.
        if (funcIdent == AlgebricksBuiltinFunctions.OR) {
            return false;
        } else if (funcIdent == AlgebricksBuiltinFunctions.AND) {
            // This is the only case that the optimizer can check the given function's arguments to see
            // if one of its argument can utilize an index.
            boolean found = false;
            for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
                ILogicalExpression argExpr = arg.getValue();
                if (argExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                    continue;
                }
                AbstractFunctionCallExpression argFuncExpr = (AbstractFunctionCallExpression) argExpr;
                boolean matchFound = analyzeFunctionExprAndUpdateAnalyzedAM(argFuncExpr, assignsAndUnnests, analyzedAMs,
                        context, typeEnvironment);
                found = found || matchFound;
            }
            return found;
        } else {
            // For single function or "NOT" case:
            return analyzeFunctionExprAndUpdateAnalyzedAM(funcExpr, assignsAndUnnests, analyzedAMs, context,
                    typeEnvironment);
        }
    }

    /**
     * Finds applicable access methods for the given function expression based
     * on the function identifier, and an analysis of the function's arguments.
     * Updates the analyzedAMs accordingly.
     *
     * @throws AlgebricksException
     */
    protected boolean analyzeFunctionExprAndUpdateAnalyzedAM(AbstractFunctionCallExpression funcExpr,
            List<AbstractLogicalOperator> assignsAndUnnests,
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs, IOptimizationContext context,
            IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();
        if (funcIdent == AlgebricksBuiltinFunctions.AND) {
            return false;
        }
        // Retrieves the list of access methods that are relevant based on the
        // funcIdent.
        List<IAccessMethod> relevantAMs = getAccessMethods().get(funcIdent);
        if (relevantAMs == null) {
            return false;
        }
        boolean atLeastOneMatchFound = false;
        // Place holder for a new analysis context in case we need one.
        AccessMethodAnalysisContext newAnalysisCtx = new AccessMethodAnalysisContext();
        for (IAccessMethod accessMethod : relevantAMs) {
            AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(accessMethod);
            // Use the current place holder.
            if (analysisCtx == null) {
                analysisCtx = newAnalysisCtx;
            }
            // Analyzes the funcExpr's arguments to see if the accessMethod is
            // truly applicable.
            boolean matchFound = accessMethod.analyzeFuncExprArgsAndUpdateAnalysisCtx(funcExpr, assignsAndUnnests,
                    analysisCtx, context, typeEnvironment);
            if (matchFound) {
                // If we've used the current new context placeholder, replace it
                // with a new one.
                if (analysisCtx == newAnalysisCtx) {
                    analyzedAMs.put(accessMethod, analysisCtx);
                    newAnalysisCtx = new AccessMethodAnalysisContext();
                }
                atLeastOneMatchFound = true;
            }
        }
        return atLeastOneMatchFound;
    }

    /**
     * Finds secondary indexes whose keys include fieldName, and adds a mapping
     * in analysisCtx.indexEsprs from that index to the a corresponding
     * optimizable function expression.
     *
     * @return true if a candidate index was added to foundIndexExprs, false
     *         otherwise
     * @throws AlgebricksException
     */
    protected boolean fillIndexExprs(List<Index> datasetIndexes, List<String> fieldName, IAType fieldType,
            IOptimizableFuncExpr optFuncExpr, int matchedFuncExprIndex, int varIdx,
            OptimizableOperatorSubTree matchedSubTree, AccessMethodAnalysisContext analysisCtx, int fieldSource)
            throws AlgebricksException {
        List<Index> indexCandidates = new ArrayList<>();
        // Add an index to the candidates if one of the indexed fields is fieldName
        for (Index index : datasetIndexes) {
            // Need to also verify the index is pending no op
            int keyIdx = index.getKeyFieldNames().indexOf(fieldName);
            List<Integer> keySources = index.getKeyFieldSourceIndicators();
            if (keyIdx >= 0 && keySourceMatches(keySources, keyIdx, fieldSource)
                    && index.getPendingOp() == MetadataUtil.PENDING_NO_OP) {
                indexCandidates.add(index);
                boolean isFieldTypeUnknown = fieldType == BuiltinType.AMISSING || fieldType == BuiltinType.ANY;
                if (isFieldTypeUnknown && (!index.isOverridingKeyFieldTypes() || index.isEnforced())) {
                    IAType indexedType = index.getKeyFieldTypes().get(keyIdx);
                    optFuncExpr.setFieldType(varIdx, indexedType);
                }
                analysisCtx.addIndexExpr(matchedSubTree.getDataset(), index, matchedFuncExprIndex, varIdx);
            }
        }
        // No index candidates for fieldName.
        if (indexCandidates.isEmpty()) {
            return false;
        }
        return true;
    }

    private static boolean keySourceMatches(List<Integer> keySources, int keyIdx, int fieldSource) {
        // TODO(ali): keySources from Index should not be null. should investigate if it can happen (ie on external ds)
        return keySources == null ? fieldSource == 0 : keySources.get(keyIdx) == fieldSource;
    }

    protected void fillAllIndexExprs(OptimizableOperatorSubTree subTree, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context) throws AlgebricksException {
        int optFuncExprIndex = 0;
        List<Index> datasetIndexes = new ArrayList<>();
        LogicalVariable datasetMetaVar = null;
        if (subTree.getDataSourceType() != DataSourceType.COLLECTION_SCAN
                && subTree.getDataSourceType() != DataSourceType.INDEXONLY_PLAN_SECONDARY_INDEX_LOOKUP) {
            datasetIndexes = metadataProvider.getDatasetIndexes(subTree.getDataset().getDataverseName(),
                    subTree.getDataset().getDatasetName());
            List<LogicalVariable> datasetVars = subTree.getDataSourceVariables();
            if (subTree.getDataset().hasMetaPart()) {
                datasetMetaVar = datasetVars.get(datasetVars.size() - 1);
            }
        }
        for (IOptimizableFuncExpr optFuncExpr : analysisCtx.getMatchedFuncExprs()) {
            // Try to match variables from optFuncExpr to assigns or unnests.
            for (int assignOrUnnestIndex = 0; assignOrUnnestIndex < subTree.getAssignsAndUnnests()
                    .size(); assignOrUnnestIndex++) {
                AbstractLogicalOperator op = subTree.getAssignsAndUnnests().get(assignOrUnnestIndex);
                if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                    analyzeAssignOp((AssignOperator) op, optFuncExpr, subTree, assignOrUnnestIndex, datasetMetaVar,
                            context, datasetIndexes, optFuncExprIndex, analysisCtx);
                } else {
                    analyzeUnnestOp((UnnestOperator) op, optFuncExpr, subTree, assignOrUnnestIndex, datasetMetaVar,
                            context, datasetIndexes, optFuncExprIndex, analysisCtx);
                }
            }

            // Try to match variables from optFuncExpr to datasourcescan if not
            // already matched in assigns.
            List<LogicalVariable> dsVarList = subTree.getDataSourceVariables();

            matchVarsFromOptFuncExprToDataSourceScan(optFuncExpr, optFuncExprIndex, datasetIndexes, dsVarList, subTree,
                    analysisCtx, context, false);

            // If there is one more datasource in the subtree, we need to scan that datasource, too.
            List<LogicalVariable> additionalDsVarList = null;

            if (subTree.hasIxJoinOuterAdditionalDataSource()) {
                additionalDsVarList = new ArrayList<>();
                for (int i = 0; i < subTree.getIxJoinOuterAdditionalDataSourceRefs().size(); i++) {
                    additionalDsVarList.addAll(subTree.getIxJoinOuterAdditionalDataSourceVariables(i));
                }

                matchVarsFromOptFuncExprToDataSourceScan(optFuncExpr, optFuncExprIndex, datasetIndexes,
                        additionalDsVarList, subTree, analysisCtx, context, true);

            }

            optFuncExprIndex++;
        }
    }

    private void analyzeUnnestOp(UnnestOperator unnestOp, IOptimizableFuncExpr optFuncExpr,
            OptimizableOperatorSubTree subTree, int assignOrUnnestIndex, LogicalVariable datasetMetaVar,
            IOptimizationContext context, List<Index> datasetIndexes, int optFuncExprIndex,
            AccessMethodAnalysisContext analysisCtx) throws AlgebricksException {
        LogicalVariable var = unnestOp.getVariable();
        int funcVarIndex = optFuncExpr.findLogicalVar(var);
        // No matching var in optFuncExpr.
        if (funcVarIndex == -1) {
            return;
        }
        // At this point we have matched the optimizable func expr
        // at optFuncExprIndex to an unnest variable.
        // Remember matching subtree.
        optFuncExpr.setOptimizableSubTree(funcVarIndex, subTree);
        List<String> fieldName = null;
        MutableInt fieldSource = new MutableInt(0);
        if (subTree.getDataSourceType() == DataSourceType.COLLECTION_SCAN) {
            VariableReferenceExpression varRef = new VariableReferenceExpression(var);
            varRef.setSourceLocation(unnestOp.getSourceLocation());
            optFuncExpr.setLogicalExpr(funcVarIndex, varRef);
        } else {
            fieldName = getFieldNameFromSubTree(optFuncExpr, subTree, assignOrUnnestIndex, 0, subTree.getRecordType(),
                    funcVarIndex, optFuncExpr.getFuncExpr().getArguments().get(funcVarIndex).getValue(),
                    subTree.getMetaRecordType(), datasetMetaVar, fieldSource);
            if (fieldName.isEmpty()) {
                return;
            }
        }
        IAType fieldType =
                (IAType) context.getOutputTypeEnvironment(unnestOp).getType(optFuncExpr.getLogicalExpr(funcVarIndex));
        // Set the fieldName in the corresponding matched function
        // expression.
        optFuncExpr.setFieldName(funcVarIndex, fieldName, fieldSource.intValue());
        optFuncExpr.setFieldType(funcVarIndex, fieldType);

        setTypeTag(context, subTree, optFuncExpr, funcVarIndex);
        if (subTree.hasDataSource()) {
            fillIndexExprs(datasetIndexes, fieldName, fieldType, optFuncExpr, optFuncExprIndex, funcVarIndex, subTree,
                    analysisCtx, fieldSource.intValue());
        }
    }

    private void analyzeAssignOp(AssignOperator assignOp, IOptimizableFuncExpr optFuncExpr,
            OptimizableOperatorSubTree subTree, int assignOrUnnestIndex, LogicalVariable datasetMetaVar,
            IOptimizationContext context, List<Index> datasetIndexes, int optFuncExprIndex,
            AccessMethodAnalysisContext analysisCtx) throws AlgebricksException {
        List<LogicalVariable> varList = assignOp.getVariables();
        MutableInt fieldSource = new MutableInt(0);
        for (int varIndex = 0; varIndex < varList.size(); varIndex++) {
            LogicalVariable var = varList.get(varIndex);
            int optVarIndex = optFuncExpr.findLogicalVar(var);
            // No matching var in optFuncExpr.
            if (optVarIndex == -1) {
                continue;
            }
            // At this point we have matched the optimizable func
            // expr at optFuncExprIndex to an assigned variable.
            // Remember matching subtree.
            optFuncExpr.setOptimizableSubTree(optVarIndex, subTree);

            fieldSource.setValue(0);
            List<String> fieldName = getFieldNameFromSubTree(optFuncExpr, subTree, assignOrUnnestIndex, varIndex,
                    subTree.getRecordType(), optVarIndex,
                    optFuncExpr.getFuncExpr().getArguments().get(optVarIndex).getValue(), subTree.getMetaRecordType(),
                    datasetMetaVar, fieldSource);

            IAType fieldType = (IAType) context.getOutputTypeEnvironment(assignOp).getVarType(var);
            // Set the fieldName in the corresponding matched
            // function expression.
            optFuncExpr.setFieldName(optVarIndex, fieldName, fieldSource.intValue());
            optFuncExpr.setFieldType(optVarIndex, fieldType);

            setTypeTag(context, subTree, optFuncExpr, optVarIndex);
            if (subTree.hasDataSource()) {
                fillIndexExprs(datasetIndexes, fieldName, fieldType, optFuncExpr, optFuncExprIndex, optVarIndex,
                        subTree, analysisCtx, fieldSource.intValue());
            }
        }
    }

    private void matchVarsFromOptFuncExprToDataSourceScan(IOptimizableFuncExpr optFuncExpr, int optFuncExprIndex,
            List<Index> datasetIndexes, List<LogicalVariable> dsVarList, OptimizableOperatorSubTree subTree,
            AccessMethodAnalysisContext analysisCtx, IOptimizationContext context, boolean fromAdditionalDataSource)
            throws AlgebricksException {
        MutableInt mutableFieldSource = new MutableInt(0);
        for (int varIndex = 0; varIndex < dsVarList.size(); varIndex++) {
            LogicalVariable var = dsVarList.get(varIndex);
            int funcVarIndex = optFuncExpr.findLogicalVar(var);
            // No matching var in optFuncExpr.
            if (funcVarIndex == -1) {
                continue;
            }
            // The variable value is one of the partitioning fields.
            List<String> fieldName = null;
            IAType fieldType = null;
            List<List<String>> subTreePKs = null;
            mutableFieldSource.setValue(0);

            if (!fromAdditionalDataSource) {
                Dataset dataset = subTree.getDataset();
                subTreePKs = dataset.getPrimaryKeys();
                // Check whether this variable is PK, not a record variable.
                if (varIndex <= subTreePKs.size() - 1) {
                    fieldName = subTreePKs.get(varIndex);
                    fieldType = (IAType) context.getOutputTypeEnvironment(subTree.getDataSourceRef().getValue())
                            .getVarType(var);
                    List<Integer> keySourceIndicators = DatasetUtil.getKeySourceIndicators(dataset);
                    if (keySourceIndicators != null) {
                        mutableFieldSource.setValue(keySourceIndicators.get(varIndex));
                    }
                }
            } else {
                // Need to check additional dataset one by one
                for (int i = 0; i < subTree.getIxJoinOuterAdditionalDatasets().size(); i++) {
                    Dataset dataset = subTree.getIxJoinOuterAdditionalDatasets().get(i);
                    if (dataset != null) {
                        subTreePKs = dataset.getPrimaryKeys();
                        // Check whether this variable is PK, not a record variable.
                        // TODO(ali): investigate why var (LogicalVariable) is looked up in subTreePKs (List<List<str>>)
                        if (subTreePKs.contains(var) && varIndex <= subTreePKs.size() - 1) {
                            fieldName = subTreePKs.get(varIndex);
                            fieldType = (IAType) context
                                    .getOutputTypeEnvironment(
                                            subTree.getIxJoinOuterAdditionalDataSourceRefs().get(i).getValue())
                                    .getVarType(var);
                            List<Integer> keySourceIndicators = DatasetUtil.getKeySourceIndicators(dataset);
                            if (keySourceIndicators != null) {
                                mutableFieldSource.setValue(keySourceIndicators.get(varIndex));
                            }
                            break;
                        }
                    }
                }
            }
            // Set the fieldName in the corresponding matched function
            // expression, and remember matching subtree.
            int fieldSource = mutableFieldSource.intValue();
            optFuncExpr.setFieldName(funcVarIndex, fieldName, fieldSource);
            optFuncExpr.setOptimizableSubTree(funcVarIndex, subTree);
            optFuncExpr.setSourceVar(funcVarIndex, var);
            VariableReferenceExpression varRef = new VariableReferenceExpression(var);
            varRef.setSourceLocation(subTree.getDataSourceRef().getValue().getSourceLocation());
            optFuncExpr.setLogicalExpr(funcVarIndex, varRef);
            setTypeTag(context, subTree, optFuncExpr, funcVarIndex);
            if (subTree.hasDataSourceScan()) {
                fillIndexExprs(datasetIndexes, fieldName, fieldType, optFuncExpr, optFuncExprIndex, funcVarIndex,
                        subTree, analysisCtx, fieldSource);
            }
        }
    }

    private void setTypeTag(IOptimizationContext context, OptimizableOperatorSubTree subTree,
            IOptimizableFuncExpr optFuncExpr, int funcVarIndex) throws AlgebricksException {
        // Set the typeTag if the type is not null
        IAType type = (IAType) context.getOutputTypeEnvironment(subTree.getRoot())
                .getVarType(optFuncExpr.getLogicalVar(funcVarIndex));
        optFuncExpr.setFieldType(funcVarIndex, type);
    }

    /**
     * Returns the field name corresponding to the assigned variable at
     * varIndex. Returns Collections.emptyList() if the expr at varIndex does not yield to a field
     * access function after following a set of allowed functions.
     *
     * @throws AlgebricksException
     */
    protected List<String> getFieldNameFromSubTree(IOptimizableFuncExpr optFuncExpr, OptimizableOperatorSubTree subTree,
            int opIndex, int assignVarIndex, ARecordType recordType, int funcVarIndex,
            ILogicalExpression parentFuncExpr, ARecordType metaType, LogicalVariable metaVar, MutableInt fieldSource)
            throws AlgebricksException {
        // Get expression corresponding to opVar at varIndex.
        AbstractLogicalExpression expr = null;
        AbstractFunctionCallExpression childFuncExpr = null;
        AbstractLogicalOperator op = subTree.getAssignsAndUnnests().get(opIndex);
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator assignOp = (AssignOperator) op;
            expr = (AbstractLogicalExpression) assignOp.getExpressions().get(assignVarIndex).getValue();
            // Can't get a field name from a constant expression. So, return null.
            if (expr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                return Collections.emptyList();
            }
            childFuncExpr = (AbstractFunctionCallExpression) expr;
        } else {
            UnnestOperator unnestOp = (UnnestOperator) op;
            expr = (AbstractLogicalExpression) unnestOp.getExpressionRef().getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return Collections.emptyList();
            }
            childFuncExpr = (AbstractFunctionCallExpression) expr;
            if (childFuncExpr.getFunctionIdentifier() != BuiltinFunctions.SCAN_COLLECTION) {
                return Collections.emptyList();
            }
            expr = (AbstractLogicalExpression) childFuncExpr.getArguments().get(0).getValue();
        }
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return Collections.emptyList();
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();

        boolean isByName = false;
        boolean isFieldAccess = false;
        String fieldName = null;
        List<String> nestedAccessFieldName = null;
        int fieldIndex = -1;
        if (funcIdent == BuiltinFunctions.FIELD_ACCESS_BY_NAME) {
            fieldName = ConstantExpressionUtil.getStringArgument(funcExpr, 1);
            if (fieldName == null) {
                return Collections.emptyList();
            }
            isFieldAccess = true;
            isByName = true;
        } else if (funcIdent == BuiltinFunctions.FIELD_ACCESS_BY_INDEX) {
            Integer idx = ConstantExpressionUtil.getIntArgument(funcExpr, 1);
            if (idx == null) {
                return Collections.emptyList();
            }
            fieldIndex = idx;
            isFieldAccess = true;
        } else if (funcIdent == BuiltinFunctions.FIELD_ACCESS_NESTED) {
            ILogicalExpression nameArg = funcExpr.getArguments().get(1).getValue();
            if (nameArg.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                return Collections.emptyList();
            }
            ConstantExpression constExpr = (ConstantExpression) nameArg;
            AOrderedList orderedNestedFieldName =
                    (AOrderedList) ((AsterixConstantValue) constExpr.getValue()).getObject();
            nestedAccessFieldName = new ArrayList<>();
            for (int i = 0; i < orderedNestedFieldName.size(); i++) {
                nestedAccessFieldName.add(((AString) orderedNestedFieldName.getItem(i)).getStringValue());
            }
            isFieldAccess = true;
            isByName = true;
        }
        if (isFieldAccess) {
            LogicalVariable sourceVar =
                    ((VariableReferenceExpression) funcExpr.getArguments().get(0).getValue()).getVariableReference();
            if (sourceVar.equals(metaVar)) {
                fieldSource.setValue(1);
            } else {
                fieldSource.setValue(0);
            }
            if (optFuncExpr != null) {
                optFuncExpr.setLogicalExpr(funcVarIndex, parentFuncExpr);
            }
            int[] assignAndExpressionIndexes = null;

            //go forward through nested assigns until you find the relevant one
            for (int i = opIndex + 1; i < subTree.getAssignsAndUnnests().size(); i++) {
                AbstractLogicalOperator subOp = subTree.getAssignsAndUnnests().get(i);
                List<LogicalVariable> varList;

                if (subOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                    //Nested was an assign
                    varList = ((AssignOperator) subOp).getVariables();
                } else if (subOp.getOperatorTag() == LogicalOperatorTag.UNNEST) {
                    //Nested is not an assign
                    varList = ((UnnestOperator) subOp).getVariables();
                } else {
                    break;
                }

                //Go through variables in assign to check for match
                for (int varIndex = 0; varIndex < varList.size(); varIndex++) {
                    LogicalVariable var = varList.get(varIndex);
                    ArrayList<LogicalVariable> parentVars = new ArrayList<>();
                    expr.getUsedVariables(parentVars);

                    if (parentVars.contains(var)) {
                        //Found the variable we are looking for.
                        //return assign and index of expression
                        int[] returnValues = { i, varIndex };
                        assignAndExpressionIndexes = returnValues;
                    }
                }
            }
            if (assignAndExpressionIndexes != null && assignAndExpressionIndexes[0] > -1) {
                //We found the nested assign

                //Recursive call on nested assign
                List<String> parentFieldNames = getFieldNameFromSubTree(optFuncExpr, subTree,
                        assignAndExpressionIndexes[0], assignAndExpressionIndexes[1], recordType, funcVarIndex,
                        parentFuncExpr, metaType, metaVar, fieldSource);

                if (parentFieldNames.isEmpty()) {
                    //Nested assign was not a field access.
                    //We will not use index
                    return Collections.emptyList();
                }

                if (!isByName) {
                    IAType subFieldType = sourceVar.equals(metaVar) ? metaType.getSubFieldType(parentFieldNames)
                            : recordType.getSubFieldType(parentFieldNames);
                    // Sub-field type can be AUnionType in case if it's optional. Thus, needs to get the actual type.
                    subFieldType = TypeComputeUtils.getActualType(subFieldType);
                    if (subFieldType.getTypeTag() != ATypeTag.OBJECT) {
                        throw CompilationException.create(ErrorCode.TYPE_CONVERT, subFieldType,
                                ARecordType.class.getName());
                    }
                    fieldName = ((ARecordType) subFieldType).getFieldNames()[fieldIndex];
                }
                if (optFuncExpr != null) {
                    optFuncExpr.setSourceVar(funcVarIndex, ((AssignOperator) op).getVariables().get(assignVarIndex));
                }
                //add fieldName to the nested fieldName, return
                if (nestedAccessFieldName != null) {
                    for (int i = 0; i < nestedAccessFieldName.size(); i++) {
                        parentFieldNames.add(nestedAccessFieldName.get(i));
                    }
                } else {
                    parentFieldNames.add(fieldName);
                }
                return (parentFieldNames);
            }

            if (optFuncExpr != null) {
                optFuncExpr.setSourceVar(funcVarIndex, ((AssignOperator) op).getVariables().get(assignVarIndex));
            }
            //no nested assign, we are at the lowest level.
            if (isByName) {
                if (nestedAccessFieldName != null) {
                    return nestedAccessFieldName;
                }
                return new ArrayList<>(Arrays.asList(fieldName));
            }
            return new ArrayList<>(Arrays.asList(sourceVar.equals(metaVar) ? metaType.getFieldNames()[fieldIndex]
                    : recordType.getFieldNames()[fieldIndex]));

        }

        if (!funcIDSetThatRetainFieldName.contains(funcIdent)) {
            return Collections.emptyList();
        }
        // We use a part of the field in edit distance computation
        if (optFuncExpr != null
                && optFuncExpr.getFuncExpr().getFunctionIdentifier() == BuiltinFunctions.EDIT_DISTANCE_CHECK) {
            optFuncExpr.setPartialField(true);
        }
        // We expect the function's argument to be a variable, otherwise we
        // cannot apply an index.
        ILogicalExpression argExpr = funcExpr.getArguments().get(0).getValue();
        if (argExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return Collections.emptyList();
        }
        LogicalVariable curVar = ((VariableReferenceExpression) argExpr).getVariableReference();
        // We look for the assign or unnest operator that produces curVar below
        // the current operator
        for (int assignOrUnnestIndex = opIndex + 1; assignOrUnnestIndex < subTree.getAssignsAndUnnests()
                .size(); assignOrUnnestIndex++) {
            AbstractLogicalOperator curOp = subTree.getAssignsAndUnnests().get(assignOrUnnestIndex);
            if (curOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                AssignOperator assignOp = (AssignOperator) curOp;
                List<LogicalVariable> varList = assignOp.getVariables();
                for (int varIndex = 0; varIndex < varList.size(); varIndex++) {
                    LogicalVariable var = varList.get(varIndex);
                    if (var.equals(curVar) && optFuncExpr != null) {
                        optFuncExpr.setSourceVar(funcVarIndex, var);
                        return getFieldNameFromSubTree(optFuncExpr, subTree, assignOrUnnestIndex, varIndex, recordType,
                                funcVarIndex, childFuncExpr, metaType, metaVar, fieldSource);
                    }
                }
            } else {
                UnnestOperator unnestOp = (UnnestOperator) curOp;
                LogicalVariable var = unnestOp.getVariable();
                if (var.equals(curVar)) {
                    getFieldNameFromSubTree(optFuncExpr, subTree, assignOrUnnestIndex, 0, recordType, funcVarIndex,
                            childFuncExpr, metaType, metaVar, fieldSource);
                }
            }
        }
        return Collections.emptyList();
    }

    /**
     * Finds the field name of each variable in the ASSIGN or UNNEST operators of the sub-tree.
     */
    protected void fillFieldNamesInTheSubTree(OptimizableOperatorSubTree subTree) throws AlgebricksException {
        LogicalVariable datasetMetaVar = null;
        if (subTree.getDataSourceType() != DataSourceType.COLLECTION_SCAN
                && subTree.getDataSourceType() != DataSourceType.INDEXONLY_PLAN_SECONDARY_INDEX_LOOKUP) {
            List<LogicalVariable> datasetVars = subTree.getDataSourceVariables();
            if (subTree.getDataset().hasMetaPart()) {
                datasetMetaVar = datasetVars.get(datasetVars.size() - 1);
            }
        }
        MutableInt fieldSource = new MutableInt(0);
        for (int assignOrUnnestIndex = 0; assignOrUnnestIndex < subTree.getAssignsAndUnnests()
                .size(); assignOrUnnestIndex++) {
            AbstractLogicalOperator op = subTree.getAssignsAndUnnests().get(assignOrUnnestIndex);
            if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                AssignOperator assignOp = (AssignOperator) op;
                List<LogicalVariable> varList = assignOp.getVariables();
                for (int varIndex = 0; varIndex < varList.size(); varIndex++) {
                    LogicalVariable var = varList.get(varIndex);
                    // funcVarIndex is not required. Thus, we set it to -1.
                    // optFuncExpr and parentFuncExpr are not required, too. Thus, we set them to null.
                    fieldSource.setValue(0);
                    List<String> fieldName = getFieldNameFromSubTree(null, subTree, assignOrUnnestIndex, varIndex,
                            subTree.getRecordType(), -1, null, subTree.getMetaRecordType(), datasetMetaVar,
                            fieldSource);
                    if (fieldName != null && !fieldName.isEmpty()) {
                        subTree.getVarsToFieldNameMap().put(var, fieldName);
                    }
                }
            } else if (op.getOperatorTag() == LogicalOperatorTag.UNNEST) {
                UnnestOperator unnestOp = (UnnestOperator) op;
                LogicalVariable var = unnestOp.getVariable();
                List<String> fieldName = null;
                if (subTree.getDataSourceType() != DataSourceType.COLLECTION_SCAN) {
                    // funcVarIndex is not required. Thus, we set it to -1.
                    // optFuncExpr and parentFuncExpr are not required, too. Thus, we set them to null.
                    fieldSource.setValue(0);
                    fieldName = getFieldNameFromSubTree(null, subTree, assignOrUnnestIndex, 0, subTree.getRecordType(),
                            -1, null, subTree.getMetaRecordType(), datasetMetaVar, fieldSource);
                    if (fieldName != null && !fieldName.isEmpty()) {
                        subTree.getVarsToFieldNameMap().put(var, fieldName);
                    }
                }
            } else {
                // unnestmap or left-outer-unnestmap?
                LeftOuterUnnestMapOperator leftOuterUnnestMapOp = null;
                UnnestMapOperator unnestMapOp = null;
                List<LogicalVariable> varList = null;

                if (op.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
                    unnestMapOp = (UnnestMapOperator) op;
                    varList = unnestMapOp.getVariables();
                } else if (op.getOperatorTag() == LogicalOperatorTag.LEFT_OUTER_UNNEST_MAP) {
                    leftOuterUnnestMapOp = (LeftOuterUnnestMapOperator) op;
                    varList = leftOuterUnnestMapOp.getVariables();
                } else {
                    continue;
                }

                for (int varIndex = 0; varIndex < varList.size(); varIndex++) {
                    LogicalVariable var = varList.get(varIndex);
                    // funcVarIndex is not required. Thus, we set it to -1.
                    // optFuncExpr and parentFuncExpr are not required, too. Thus, we set them to null.
                    fieldSource.setValue(0);
                    List<String> fieldName = getFieldNameFromSubTree(null, subTree, assignOrUnnestIndex, varIndex,
                            subTree.getRecordType(), -1, null, subTree.getMetaRecordType(), datasetMetaVar,
                            fieldSource);
                    if (fieldName != null && !fieldName.isEmpty()) {
                        subTree.getVarsToFieldNameMap().put(var, fieldName);
                    }
                }
            }
        }

        // DatasourceScan?
        if (subTree.hasDataSourceScan()) {
            List<LogicalVariable> primaryKeyVarList = new ArrayList<>();

            if (subTree.getDataset().getDatasetType() == DatasetType.INTERNAL) {
                subTree.getPrimaryKeyVars(null, primaryKeyVarList);

                Index primaryIndex = getPrimaryIndexFromDataSourceScanOp(subTree.getDataSourceRef().getValue());

                for (int i = 0; i < primaryKeyVarList.size(); i++) {
                    subTree.getVarsToFieldNameMap().put(primaryKeyVarList.get(i),
                            primaryIndex.getKeyFieldNames().get(i));
                }
            }

        }

    }

    /**
     * Fetches the associated primary index from the given DATASOURCESCAN operator.
     */
    protected Index getPrimaryIndexFromDataSourceScanOp(ILogicalOperator dataSourceScanOp) throws AlgebricksException {
        if (dataSourceScanOp.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
            return null;
        }
        Pair<String, String> datasetInfo = AnalysisUtil.getDatasetInfo((DataSourceScanOperator) dataSourceScanOp);
        String dataverseName = datasetInfo.first;
        String datasetName = datasetInfo.second;

        Index idxUsedInUnnestMap = metadataProvider.getIndex(dataverseName, datasetName, datasetName);
        return idxUsedInUnnestMap;
    }

}
