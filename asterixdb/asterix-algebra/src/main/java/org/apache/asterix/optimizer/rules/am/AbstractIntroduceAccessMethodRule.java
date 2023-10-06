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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.dataflow.data.common.ExpressionTypeComputer;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.ArrayIndexUtil;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.optimizer.rules.am.OptimizableOperatorSubTree.DataSourceType;
import org.apache.asterix.optimizer.rules.util.FullTextUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
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

import com.google.common.base.Strings;

/**
 * Class that embodies the commonalities between rewrite rules for access
 * methods.
 */
public abstract class AbstractIntroduceAccessMethodRule implements IAlgebraicRewriteRule {
    protected MetadataProvider metadataProvider;

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
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs, IOptimizationContext context,
            boolean isJoinLeftBranch) throws AlgebricksException {
        fillSubTreeIndexExprs(subTree, analyzedAMs, context, isJoinLeftBranch, false);
    }

    /**
     * Fills the information about the given optimizable function expressions using the subtree.
     *
     * @param subTree
     * @param analyzedAMs
     * @param context
     * @param isJoinLeftBranch
     * @param isArbitraryFormOfSubtree if the given subtree is in an arbitrary form that OptimizableSubTree class can't initialize, we try
     *                                 to fill the field type of each variable that is used in the optimizable function expressions.
     *                                 This way, an index-nested-loop-join transformation can be conducted properly since the transformation
     *                                 process skips an optimzable function expression if the field type of one of its variable is unknown.
     * @throws AlgebricksException
     */
    protected void fillSubTreeIndexExprs(OptimizableOperatorSubTree subTree,
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs, IOptimizationContext context,
            boolean isJoinLeftBranch, boolean isArbitraryFormOfSubtree) throws AlgebricksException {
        Iterator<Map.Entry<IAccessMethod, AccessMethodAnalysisContext>> amIt = analyzedAMs.entrySet().iterator();
        // Check applicability of indexes by access method type.
        while (amIt.hasNext()) {
            Map.Entry<IAccessMethod, AccessMethodAnalysisContext> entry = amIt.next();
            AccessMethodAnalysisContext amCtx = entry.getValue();
            // For the current access method type, map variables to applicable
            // indexes.
            if (isArbitraryFormOfSubtree) {
                fillVarFieldTypeForOptFuncExprs(subTree, amCtx, context);
            } else {
                if (isJoinLeftBranch) {
                    fillVarFieldTypeForOptFuncExprs(subTree, amCtx, context);
                }
                fillAllIndexExprs(subTree, amCtx, context, entry.getKey());
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
            IOptimizationContext context, IVariableTypeEnvironment typeEnvironment, boolean checkApplicableOnly)
            throws AlgebricksException {
        Iterator<Map.Entry<IAccessMethod, AccessMethodAnalysisContext>> amIt = analyzedAMs.entrySet().iterator();
        // Check applicability of indexes by access method type.
        while (amIt.hasNext()) {
            Map.Entry<IAccessMethod, AccessMethodAnalysisContext> entry = amIt.next();
            AccessMethodAnalysisContext amCtx = entry.getValue();
            pruneIndexCandidates(entry.getKey(), amCtx, context, typeEnvironment, checkApplicableOnly);
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
        List<Pair<IAccessMethod, Index>> list = new ArrayList<>();
        chooseAllIndexes(analyzedAMs, list);
        return list.isEmpty() ? null : list.get(0);
    }

    /**
     * Choose all indexes that match the given access method. These indexes will be used as index-search
     * to replace the given predicates in a SELECT operator. Also, if there are multiple same type of indexes
     * on the same field, only one of them will be chosen. Allowed cases (AccessMethod, IndexType) are:
     * [BTreeAccessMethod , IndexType.BTREE], [RTreeAccessMethod , IndexType.RTREE],
     * [InvertedIndexAccessMethod, IndexType.SINGLE_PARTITION_WORD_INVIX || SINGLE_PARTITION_NGRAM_INVIX ||
     * LENGTH_PARTITIONED_WORD_INVIX || LENGTH_PARTITIONED_NGRAM_INVIX]
     */
    protected void chooseAllIndexes(Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs,
            List<Pair<IAccessMethod, Index>> result) {
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
                boolean isKeywordIndexChosen = indexType == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                        || indexType == IndexType.SINGLE_PARTITION_WORD_INVIX;
                boolean isNgramIndexChosen = indexType == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX
                        || indexType == IndexType.SINGLE_PARTITION_NGRAM_INVIX;
                if ((chosenAccessMethod == BTreeAccessMethod.INSTANCE && indexType == IndexType.BTREE)
                        || (chosenAccessMethod == ArrayBTreeAccessMethod.INSTANCE && indexType == IndexType.ARRAY)
                        || (chosenAccessMethod == RTreeAccessMethod.INSTANCE && indexType == IndexType.RTREE)
                        // the inverted index will be utilized
                        // For Ngram, the full-text config used in the index and in the query are always the default one,
                        // so we don't check if the full-text config in the index and query match
                        //
                        // Note that the ngram index can be used in both
                        // 1) full-text ftcontains() function
                        // 2) non-full-text, regular string contains() function
                        // 3) edit-distance functions that take keyword as an argument,
                        //     e.g. edit_distance_check() when the threshold is larger than 1
                        || (chosenAccessMethod == InvertedIndexAccessMethod.INSTANCE && isNgramIndexChosen)
                        // the inverted index will be utilized
                        // For keyword, different full-text configs may apply to different indexes on the same field,
                        // so we need to check if the config used in the index matches the config in the ftcontains() query
                        // If not, then we cannot use this index.
                        //
                        // Note that for now, the keyword/fulltext index can be utilized in
                        // 1) the full-text ftcontains() function
                        // 2) functions that take keyword as an argument, e.g. edit_distance_check() when the threshold is 1
                        || (chosenAccessMethod == InvertedIndexAccessMethod.INSTANCE && isKeywordIndexChosen
                                && isSameFullTextConfigInIndexAndQuery(analysisCtx, chosenIndex.getIndexDetails()))) {

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
    }

    private boolean isSameFullTextConfigInIndexAndQuery(AccessMethodAnalysisContext analysisCtx,
            Index.IIndexDetails indexDetails) {
        String indexFullTextConfig = ((Index.TextIndexDetails) indexDetails).getFullTextConfigName();

        IOptimizableFuncExpr expr = analysisCtx.getMatchedFuncExpr(0);
        if (FullTextUtil.isFullTextContainsFunctionExpr(expr)) {
            // ftcontains()
            String expectedConfig = FullTextUtil.getFullTextConfigNameFromExpr(expr);
            if (Strings.isNullOrEmpty(expectedConfig)) {
                return Strings.isNullOrEmpty(indexFullTextConfig);
            } else if (expectedConfig.equals(indexFullTextConfig)) {
                return true;
            }
        } else {
            // besides ftcontains(), there are other functions that utilize the full-text inverted-index,
            // e.g. edit_distance_check(),
            // for now, we don't accept users to specify the full-text config in those functions,
            // that means, we assume the full-text config used in those function is always the default one with the name null,
            // and if the index full-text config name is also null, the index can be utilized
            if (Strings.isNullOrEmpty(indexFullTextConfig)) {
                return true;
            }
        }
        return false;
    }

    protected List<List<String>> findKeyFieldNames(Index index) throws CompilationException {
        List<List<String>> keyFieldNames = new ArrayList<>();
        DatasetConfig.IndexType indexType = index.getIndexType();
        switch (Index.IndexCategory.of(indexType)) {
            case ARRAY:
                Index.ArrayIndexDetails arrayIndexDetails = (Index.ArrayIndexDetails) index.getIndexDetails();
                for (Index.ArrayIndexElement e : arrayIndexDetails.getElementList()) {
                    for (int i = 0; i < e.getProjectList().size(); i++) {
                        List<String> project = e.getProjectList().get(i);
                        keyFieldNames.add(ArrayIndexUtil.getFlattenedKeyFieldNames(e.getUnnestList(), project));
                    }
                }
                break;
            case VALUE:
                Index.ValueIndexDetails valueIndexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
                keyFieldNames = valueIndexDetails.getKeyFieldNames();
                break;
            case TEXT:
                Index.TextIndexDetails textIndexDetails = (Index.TextIndexDetails) index.getIndexDetails();
                keyFieldNames = textIndexDetails.getKeyFieldNames();
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_INDEX_TYPE, String.valueOf(indexType));
        }

        return keyFieldNames;
    }

    protected List<IAType> findKeyTypes(Index index) throws CompilationException {
        List<IAType> keyFieldTypes = new ArrayList<>();
        DatasetConfig.IndexType indexType = index.getIndexType();
        switch (Index.IndexCategory.of(indexType)) {
            case ARRAY:
                Index.ArrayIndexDetails arrayIndexDetails = (Index.ArrayIndexDetails) index.getIndexDetails();
                for (Index.ArrayIndexElement e : arrayIndexDetails.getElementList()) {
                    for (int i = 0; i < e.getProjectList().size(); i++) {
                        List<String> project = e.getProjectList().get(i);
                        keyFieldTypes.add(e.getTypeList().get(i));
                    }
                }
                break;
            case VALUE:
                Index.ValueIndexDetails valueIndexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
                keyFieldTypes = valueIndexDetails.getKeyFieldTypes();
                break;
            case TEXT:
                Index.TextIndexDetails textIndexDetails = (Index.TextIndexDetails) index.getIndexDetails();
                keyFieldTypes = textIndexDetails.getKeyFieldTypes();
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_INDEX_TYPE, String.valueOf(indexType));
        }

        return keyFieldTypes;

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
            IOptimizationContext context, IVariableTypeEnvironment typeEnvironment, boolean checkApplicableOnly)
            throws AlgebricksException {
        Iterator<Map.Entry<Index, List<Pair<Integer, Integer>>>> indexExprAndVarIt =
                analysisCtx.getIteratorForIndexExprsAndVars();
        boolean hasIndexPreferences = false;
        ArrayList<Integer> matchedExpressions = new ArrayList<>();
        while (indexExprAndVarIt.hasNext()) {
            Map.Entry<Index, List<Pair<Integer, Integer>>> indexExprAndVarEntry = indexExprAndVarIt.next();
            Index index = indexExprAndVarEntry.getKey();
            IndexType indexType = index.getIndexType();
            if (!accessMethod.matchIndexType(indexType)) {
                indexExprAndVarIt.remove();
                continue;
            }
            List<List<String>> keyFieldNames = findKeyFieldNames(index);
            List<IAType> keyFieldTypes = findKeyTypes(index);

            boolean allUsed = true;
            int lastFieldMatched = -1;
            matchedExpressions.clear();
            // Used to keep track of matched expressions (added for prefix search)
            int numMatchedKeys = 0;

            for (int i = 0; i < keyFieldNames.size(); i++) {
                List<String> keyField = keyFieldNames.get(i);
                final IAType keyType = keyFieldTypes.get(i);
                boolean foundKeyField = false;
                Iterator<Pair<Integer, Integer>> exprsAndVarIter = indexExprAndVarEntry.getValue().iterator();
                while (exprsAndVarIter.hasNext()) {
                    final Pair<Integer, Integer> exprAndVarIdx = exprsAndVarIter.next();
                    final IOptimizableFuncExpr optFuncExpr = analysisCtx.getMatchedFuncExpr(exprAndVarIdx.first);
                    // If expr is not optimizable by concrete index then remove
                    // expr and continue.
                    if (!accessMethod.exprIsOptimizable(index, optFuncExpr, checkApplicableOnly)) {
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
                                public Object getVarType(LogicalVariable var,
                                        List<LogicalVariable> nonMissableVariables,
                                        List<List<LogicalVariable>> correlatedMissableVariableLists,
                                        List<LogicalVariable> nonNullableVariables,
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
                    if (typeMatch && optFuncExpr.findFieldName(keyField) != -1
                            && optFuncExpr.getOperatorSubTree(exprAndVarIdx.second).hasDataSourceScan()) {
                        foundKeyField = true;
                        matchedExpressions.add(exprAndVarIdx.first);
                        hasIndexPreferences =
                                hasIndexPreferences || accessMethod.getSecondaryIndexPreferences(optFuncExpr) != null;
                    }
                }
                if (foundKeyField) {
                    numMatchedKeys++;
                    if (lastFieldMatched == i - 1) {
                        lastFieldMatched = i;
                    }
                } else {
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
            if (!allUsed && accessMethod.matchAllIndexExprs(index)) {
                indexExprAndVarIt.remove();
                continue;
            }
            // A prefix of the index exprs may have been matched.
            if (accessMethod.matchPrefixIndexExprs(index)) {
                if (lastFieldMatched < 0) {
                    indexExprAndVarIt.remove();
                    continue;

                } else if (Index.IndexCategory.of(indexType).equals(Index.IndexCategory.ARRAY)) {
                    // For array indexes, we cannot make the decision to apply the prefix until we see a conjunct
                    // conditioning on an array. We should improve using array indexes for queries that don't involve
                    // the array component in the future.
                    Index.ArrayIndexDetails arrayIndexDetails = (Index.ArrayIndexDetails) index.getIndexDetails();
                    int indexOfFirstArrayField = 0;
                    for (Index.ArrayIndexElement e : arrayIndexDetails.getElementList()) {
                        if (!e.getUnnestList().isEmpty()) {
                            break;
                        }
                        for (List<String> ignored : e.getProjectList()) {
                            indexOfFirstArrayField++;
                        }
                    }
                    if (lastFieldMatched < indexOfFirstArrayField) {
                        indexExprAndVarIt.remove();
                        continue;
                    }
                }
            }
            analysisCtx.putNumberOfMatchedKeys(index, numMatchedKeys);
        }

        if (hasIndexPreferences) {
            Collection<Index> preferredSecondaryIndexes = fetchSecondaryIndexPreferences(accessMethod, analysisCtx);
            if (preferredSecondaryIndexes != null) {
                // if we have preferred indexes then remove all non-preferred indexes
                removeNonPreferredSecondaryIndexes(analysisCtx, preferredSecondaryIndexes);
            }
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

    private Set<Index> fetchSecondaryIndexPreferences(IAccessMethod accessMethod,
            AccessMethodAnalysisContext analysisCtx) {
        Set<Index> preferredSecondaryIndexes = null;
        for (Iterator<Map.Entry<Index, List<Pair<Integer, Integer>>>> indexExprAndVarIt =
                analysisCtx.getIteratorForIndexExprsAndVars(); indexExprAndVarIt.hasNext();) {
            Map.Entry<Index, List<Pair<Integer, Integer>>> indexExprAndVarEntry = indexExprAndVarIt.next();
            Index index = indexExprAndVarEntry.getKey();
            if (index.isSecondaryIndex()) {
                for (Pair<Integer, Integer> exprVarPair : indexExprAndVarEntry.getValue()) {
                    IOptimizableFuncExpr optFuncExpr = analysisCtx.getMatchedFuncExpr(exprVarPair.first);
                    Collection<String> preferredIndexNames = accessMethod.getSecondaryIndexPreferences(optFuncExpr);
                    if (preferredIndexNames != null && preferredIndexNames.contains(index.getIndexName())) {
                        if (preferredSecondaryIndexes == null) {
                            preferredSecondaryIndexes = new HashSet<>();
                        }
                        preferredSecondaryIndexes.add(index);
                        break;
                    }
                }
            }
        }
        return preferredSecondaryIndexes;
    }

    private void removeNonPreferredSecondaryIndexes(AccessMethodAnalysisContext analysisCtx,
            Collection<Index> preferredIndexes) {
        for (Iterator<Map.Entry<Index, List<Pair<Integer, Integer>>>> indexExprAndVarIt =
                analysisCtx.getIteratorForIndexExprsAndVars(); indexExprAndVarIt.hasNext();) {
            Map.Entry<Index, List<Pair<Integer, Integer>>> indexExprAndVarEntry = indexExprAndVarIt.next();
            Index index = indexExprAndVarEntry.getKey();
            if (index.isSecondaryIndex() && !preferredIndexes.contains(index)) {
                indexExprAndVarIt.remove();
            }
        }
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
     * otherwise
     * @throws AlgebricksException
     */
    protected boolean fillIndexExprs(List<Index> datasetIndexes, List<String> fieldName, IAType fieldType,
            IOptimizableFuncExpr optFuncExpr, int matchedFuncExprIndex, int varIdx,
            OptimizableOperatorSubTree matchedSubTree, AccessMethodAnalysisContext analysisCtx, int fieldSource,
            IAccessMethod accessMethod) throws AlgebricksException {
        List<Index> indexCandidates = new ArrayList<>();
        // Add an index to the candidates if one of the indexed fields is fieldName
        for (Index index : datasetIndexes) {
            if (!accessMethod.matchIndexType(index.getIndexType())) {
                continue;
            }
            List<List<String>> keyFieldNames;
            List<IAType> keyFieldTypes;
            List<Integer> keySources;
            boolean isOverridingKeyFieldTypes;
            boolean hasCastDefaultNull = false;
            switch (Index.IndexCategory.of(index.getIndexType())) {
                case ARRAY:
                    Index.ArrayIndexDetails arrayIndexDetails = (Index.ArrayIndexDetails) index.getIndexDetails();
                    keyFieldNames = new ArrayList<>();
                    keyFieldTypes = new ArrayList<>();
                    keySources = new ArrayList<>();
                    for (Index.ArrayIndexElement e : arrayIndexDetails.getElementList()) {
                        for (int i = 0; i < e.getProjectList().size(); i++) {
                            List<String> project = e.getProjectList().get(i);
                            keyFieldNames.add(ArrayIndexUtil.getFlattenedKeyFieldNames(e.getUnnestList(), project));
                            keyFieldTypes.add(e.getTypeList().get(i).getType());
                            keySources.add(e.getSourceIndicator());
                        }
                    }
                    isOverridingKeyFieldTypes = arrayIndexDetails.isOverridingKeyFieldTypes();
                    break;
                case VALUE:
                    Index.ValueIndexDetails valueIndexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
                    keyFieldNames = valueIndexDetails.getKeyFieldNames();
                    keyFieldTypes = valueIndexDetails.getKeyFieldTypes();
                    keySources = valueIndexDetails.getKeyFieldSourceIndicators();
                    isOverridingKeyFieldTypes = valueIndexDetails.isOverridingKeyFieldTypes();
                    hasCastDefaultNull = valueIndexDetails.getCastDefaultNull().getOrElse(false);
                    break;
                case TEXT:
                    Index.TextIndexDetails textIndexDetails = (Index.TextIndexDetails) index.getIndexDetails();
                    keyFieldNames = textIndexDetails.getKeyFieldNames();
                    keyFieldTypes = textIndexDetails.getKeyFieldTypes();
                    keySources = textIndexDetails.getKeyFieldSourceIndicators();
                    isOverridingKeyFieldTypes = textIndexDetails.isOverridingKeyFieldTypes();
                    break;
                default:
                    throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_INDEX_TYPE,
                            String.valueOf(index.getIndexType()));
            }
            // Need to also verify the index is pending no op
            int keyIdx = keyFieldNames.indexOf(fieldName);
            if (keyIdx >= 0 && keySourceMatches(keySources, keyIdx, fieldSource)
                    && index.getPendingOp() == MetadataUtil.PENDING_NO_OP) {
                IAType indexedType = keyFieldTypes.get(keyIdx);
                List<AbstractFunctionCallExpression> stepExprs = optFuncExpr.getStepsExprs(varIdx);
                if (acceptSteps(index, accessMethod, stepExprs, indexedType, hasCastDefaultNull)) {
                    indexCandidates.add(index);
                    boolean isFieldTypeUnknown = fieldType == BuiltinType.AMISSING || fieldType == BuiltinType.ANY;
                    if (isFieldTypeUnknown && (!isOverridingKeyFieldTypes || index.isEnforced())) {
                        optFuncExpr.setFieldType(varIdx, indexedType);
                    }
                    analysisCtx.addIndexExpr(matchedSubTree.getDataset(), index, matchedFuncExprIndex, varIdx);
                }
            }
        }
        // No index candidates for fieldName.
        if (indexCandidates.isEmpty()) {
            return false;
        }
        return true;
    }

    private boolean acceptSteps(Index index, IAccessMethod accessMethod, List<AbstractFunctionCallExpression> stepExprs,
            IAType indexedType, boolean indexHasCastDefaultNull) throws AlgebricksException {
        for (int i = stepExprs.size() - 1; i >= 0; i--) {
            if (!accessMethod.acceptsFunction(stepExprs.get(i), index, indexedType, indexHasCastDefaultNull, i == 0)) {
                return false;
            }
        }
        return true;
    }

    private static boolean keySourceMatches(List<Integer> keySources, int keyIdx, int fieldSource) {
        // TODO(ali): keySources from Index should not be null. should investigate if it can happen (ie on external ds)
        return keySources == null ? fieldSource == 0 : keySources.get(keyIdx) == fieldSource;
    }

    protected void fillAllIndexExprs(OptimizableOperatorSubTree subTree, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, IAccessMethod accessMethod) throws AlgebricksException {
        int optFuncExprIndex = 0;
        List<Index> datasetIndexes = new ArrayList<>();
        LogicalVariable datasetMetaVar = null;
        if (subTree.getDataSourceType() != DataSourceType.COLLECTION_SCAN
                && subTree.getDataSourceType() != DataSourceType.INDEXONLY_PLAN_SECONDARY_INDEX_LOOKUP) {
            datasetIndexes = metadataProvider.getDatasetIndexes(subTree.getDataset().getDatabaseName(),
                    subTree.getDataset().getDataverseName(), subTree.getDataset().getDatasetName());
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
                            context, datasetIndexes, optFuncExprIndex, analysisCtx, accessMethod);
                } else {
                    analyzeUnnestOp((UnnestOperator) op, optFuncExpr, subTree, assignOrUnnestIndex, datasetMetaVar,
                            context, datasetIndexes, optFuncExprIndex, analysisCtx, accessMethod);
                }
            }

            // Try to match variables from optFuncExpr to datasourcescan if not
            // already matched in assigns.
            List<LogicalVariable> dsVarList = subTree.getDataSourceVariables();

            matchVarsFromOptFuncExprToDataSourceScan(optFuncExpr, optFuncExprIndex, datasetIndexes, dsVarList, subTree,
                    analysisCtx, context, false, accessMethod);

            // If there is one more datasource in the subtree, we need to scan that datasource, too.
            List<LogicalVariable> additionalDsVarList = null;

            if (subTree.hasIxJoinOuterAdditionalDataSource()) {
                additionalDsVarList = new ArrayList<>();
                for (int i = 0; i < subTree.getIxJoinOuterAdditionalDataSourceRefs().size(); i++) {
                    additionalDsVarList.addAll(subTree.getIxJoinOuterAdditionalDataSourceVariables(i));
                }

                matchVarsFromOptFuncExprToDataSourceScan(optFuncExpr, optFuncExprIndex, datasetIndexes,
                        additionalDsVarList, subTree, analysisCtx, context, true, accessMethod);

            }

            optFuncExprIndex++;
        }
    }

    private void analyzeUnnestOp(UnnestOperator unnestOp, IOptimizableFuncExpr optFuncExpr,
            OptimizableOperatorSubTree subTree, int assignOrUnnestIndex, LogicalVariable datasetMetaVar,
            IOptimizationContext context, List<Index> datasetIndexes, int optFuncExprIndex,
            AccessMethodAnalysisContext analysisCtx, IAccessMethod accessMethod) throws AlgebricksException {
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
        int fieldSource = 0;
        if (subTree.getDataSourceType() == DataSourceType.COLLECTION_SCAN) {
            ILogicalExpression expr = optFuncExpr.getArgument(funcVarIndex).getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                optFuncExpr.addStepExpr(funcVarIndex, ((AbstractFunctionCallExpression) expr));
            }
            optFuncExpr.setLogicalExpr(funcVarIndex, expr);
        } else {
            if (subTree.getDataSourceType() == DataSourceType.DATASOURCE_SCAN) {
                subTree.setLastMatchedDataSourceVars(0, funcVarIndex);
            }
            Pair<List<String>, Integer> fieldNameAndSource =
                    AccessMethodUtils.getFieldNameSetStepsFromSubTree(optFuncExpr, subTree, assignOrUnnestIndex, 0,
                            funcVarIndex, optFuncExpr.getArgument(funcVarIndex).getValue(), context);
            fieldName = fieldNameAndSource.first;
            fieldSource = fieldNameAndSource.second;
            if (fieldName.isEmpty()) {
                return;
            }
        }
        IAType fieldType =
                (IAType) context.getOutputTypeEnvironment(unnestOp).getType(optFuncExpr.getLogicalExpr(funcVarIndex));
        // Set the fieldName in the corresponding matched function
        // expression.
        optFuncExpr.setFieldName(funcVarIndex, fieldName, fieldSource);
        optFuncExpr.setFieldType(funcVarIndex, fieldType);

        setTypeTag(context, subTree, optFuncExpr, funcVarIndex);
        if (subTree.hasDataSource()) {
            fillIndexExprs(datasetIndexes, fieldName, fieldType, optFuncExpr, optFuncExprIndex, funcVarIndex, subTree,
                    analysisCtx, fieldSource, accessMethod);
        }
    }

    private void analyzeAssignOp(AssignOperator assignOp, IOptimizableFuncExpr optFuncExpr,
            OptimizableOperatorSubTree subTree, int assignOrUnnestIndex, LogicalVariable datasetMetaVar,
            IOptimizationContext context, List<Index> datasetIndexes, int optFuncExprIndex,
            AccessMethodAnalysisContext analysisCtx, IAccessMethod accessMethod) throws AlgebricksException {
        boolean doesArrayIndexQualify = context.getPhysicalOptimizationConfig().isArrayIndexEnabled()
                && datasetIndexes.stream().anyMatch(i -> i.getIndexType() == IndexType.ARRAY);
        List<LogicalVariable> varList = assignOp.getVariables();
        for (int varIndex = 0; varIndex < varList.size(); varIndex++) {
            LogicalVariable var = varList.get(varIndex);
            int optVarIndex = optFuncExpr.findLogicalVar(var);
            if (optVarIndex == -1) {
                if (doesArrayIndexQualify && subTree.getDataSourceType() == DataSourceType.DATASOURCE_SCAN) {
                    // We may be able to apply an array index to this variable.
                    Triple<Integer, List<String>, IAType> fieldTriplet = AccessMethodUtils
                            .analyzeVarForArrayIndexes(datasetIndexes, optFuncExpr, subTree, context, var, analysisCtx);
                    if (fieldTriplet != null && subTree.hasDataSource()) {
                        fillIndexExprs(datasetIndexes, fieldTriplet.second, fieldTriplet.third, optFuncExpr,
                                optFuncExprIndex, fieldTriplet.first, subTree, analysisCtx, 0, accessMethod);
                    }
                }
                continue;
            }
            // At this point we have matched the optimizable func
            // expr at optFuncExprIndex to an assigned variable.
            // Remember matching subtree.
            optFuncExpr.setOptimizableSubTree(optVarIndex, subTree);
            if (subTree.getDataSourceType() == DataSourceType.DATASOURCE_SCAN) {
                subTree.setLastMatchedDataSourceVars(varIndex, optVarIndex);
            }

            Pair<List<String>, Integer> fieldNameAndSource =
                    AccessMethodUtils.getFieldNameSetStepsFromSubTree(optFuncExpr, subTree, assignOrUnnestIndex,
                            varIndex, optVarIndex, optFuncExpr.getArgument(optVarIndex).getValue(), context);
            List<String> fieldName = fieldNameAndSource.first;
            int fieldSource = fieldNameAndSource.second;

            IAType fieldType = (IAType) context.getOutputTypeEnvironment(assignOp).getVarType(var);
            // Set the fieldName in the corresponding matched
            // function expression.
            optFuncExpr.setFieldName(optVarIndex, fieldName, fieldSource);
            optFuncExpr.setFieldType(optVarIndex, fieldType);

            setTypeTag(context, subTree, optFuncExpr, optVarIndex);
            if (subTree.hasDataSource()) {
                fillIndexExprs(datasetIndexes, fieldName, fieldType, optFuncExpr, optFuncExprIndex, optVarIndex,
                        subTree, analysisCtx, fieldSource, accessMethod);
            }
        }
    }

    private void matchVarsFromOptFuncExprToDataSourceScan(IOptimizableFuncExpr optFuncExpr, int optFuncExprIndex,
            List<Index> datasetIndexes, List<LogicalVariable> dsVarList, OptimizableOperatorSubTree subTree,
            AccessMethodAnalysisContext analysisCtx, IOptimizationContext context, boolean fromAdditionalDataSource,
            IAccessMethod accessMethod) throws AlgebricksException {
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
            ILogicalExpression expr = optFuncExpr.getArgument(funcVarIndex).getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                optFuncExpr.addStepExpr(funcVarIndex, ((AbstractFunctionCallExpression) expr));
            }
            optFuncExpr.setLogicalExpr(funcVarIndex, expr);
            setTypeTag(context, subTree, optFuncExpr, funcVarIndex);
            if (subTree.hasDataSourceScan()) {
                fillIndexExprs(datasetIndexes, fieldName, fieldType, optFuncExpr, optFuncExprIndex, funcVarIndex,
                        subTree, analysisCtx, fieldSource, accessMethod);
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
     * Finds the field name of each variable in the ASSIGN or UNNEST operators of the sub-tree.
     */
    protected void fillFieldNamesInTheSubTree(OptimizableOperatorSubTree subTree, IOptimizationContext context)
            throws AlgebricksException {
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
                    List<String> fieldName = AccessMethodUtils.getFieldNameSetStepsFromSubTree(null, subTree,
                            assignOrUnnestIndex, varIndex, -1, null, context).first;
                    if (fieldName != null && !fieldName.isEmpty()) {
                        subTree.getVarsToFieldNameMap().put(var, fieldName);
                    }
                }
            } else if (op.getOperatorTag() == LogicalOperatorTag.UNNEST) {
                UnnestOperator unnestOp = (UnnestOperator) op;
                LogicalVariable var = unnestOp.getVariable();
                if (subTree.getDataSourceType() != DataSourceType.COLLECTION_SCAN) {
                    // funcVarIndex is not required. Thus, we set it to -1.
                    // optFuncExpr and parentFuncExpr are not required, too. Thus, we set them to null.
                    List<String> fieldName = AccessMethodUtils.getFieldNameSetStepsFromSubTree(null, subTree,
                            assignOrUnnestIndex, 0, -1, null, context).first;
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
                    List<String> fieldName = AccessMethodUtils.getFieldNameSetStepsFromSubTree(null, subTree,
                            assignOrUnnestIndex, varIndex, -1, null, context).first;
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
                List<List<String>> keyFieldNames =
                        ((Index.ValueIndexDetails) primaryIndex.getIndexDetails()).getKeyFieldNames();
                for (int i = 0; i < primaryKeyVarList.size(); i++) {
                    subTree.getVarsToFieldNameMap().put(primaryKeyVarList.get(i), keyFieldNames.get(i));
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
        DataSourceId srcId = (DataSourceId) ((DataSourceScanOperator) dataSourceScanOp).getDataSource().getId();
        String database = srcId.getDatabaseName();
        DataverseName dataverseName = srcId.getDataverseName();
        String datasourceName = srcId.getDatasourceName();
        return metadataProvider.getIndex(database, dataverseName, datasourceName, datasourceName);
    }
}
