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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.dataflow.data.common.AqlExpressionTypeComputer;
import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.optimizer.rules.am.OptimizableOperatorSubTree.DataSourceType;
import org.apache.commons.lang3.mutable.Mutable;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Class that embodies the commonalities between rewrite rules for access
 * methods.
 */
public abstract class AbstractIntroduceAccessMethodRule implements IAlgebraicRewriteRule {

    private AqlMetadataProvider metadataProvider;

    public abstract Map<FunctionIdentifier, List<IAccessMethod>> getAccessMethods();

    protected static void registerAccessMethod(IAccessMethod accessMethod,
            Map<FunctionIdentifier, List<IAccessMethod>> accessMethods) {
        List<FunctionIdentifier> funcs = accessMethod.getOptimizableFunctions();
        for (FunctionIdentifier funcIdent : funcs) {
            List<IAccessMethod> l = accessMethods.get(funcIdent);
            if (l == null) {
                l = new ArrayList<IAccessMethod>();
                accessMethods.put(funcIdent, l);
            }
            l.add(accessMethod);
        }
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    protected void setMetadataDeclarations(IOptimizationContext context) {
        metadataProvider = (AqlMetadataProvider) context.getMetadataProvider();
    }

    protected void fillSubTreeIndexExprs(OptimizableOperatorSubTree subTree,
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs, IOptimizationContext context)
                    throws AlgebricksException {
        Iterator<Map.Entry<IAccessMethod, AccessMethodAnalysisContext>> amIt = analyzedAMs.entrySet().iterator();
        // Check applicability of indexes by access method type.
        while (amIt.hasNext()) {
            Map.Entry<IAccessMethod, AccessMethodAnalysisContext> entry = amIt.next();
            AccessMethodAnalysisContext amCtx = entry.getValue();
            // For the current access method type, map variables to applicable
            // indexes.
            fillAllIndexExprs(subTree, amCtx, context);
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
            if (amCtx.indexExprsAndVars.isEmpty()) {
                amIt.remove();
            }
        }
    }

    /**
     * Simply picks the first index that it finds. TODO: Improve this decision
     * process by making it more systematic.
     */
    protected Pair<IAccessMethod, Index> chooseIndex(Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs) {
        Iterator<Map.Entry<IAccessMethod, AccessMethodAnalysisContext>> amIt = analyzedAMs.entrySet().iterator();
        while (amIt.hasNext()) {
            Map.Entry<IAccessMethod, AccessMethodAnalysisContext> amEntry = amIt.next();
            AccessMethodAnalysisContext analysisCtx = amEntry.getValue();
            Iterator<Map.Entry<Index, List<Pair<Integer, Integer>>>> indexIt = analysisCtx.indexExprsAndVars.entrySet()
                    .iterator();
            if (indexIt.hasNext()) {
                Map.Entry<Index, List<Pair<Integer, Integer>>> indexEntry = indexIt.next();
                // To avoid a case where the chosen access method and a chosen
                // index type is different.
                // Allowed Case: [BTreeAccessMethod , IndexType.BTREE],
                //               [RTreeAccessMethod , IndexType.RTREE],
                //               [InvertedIndexAccessMethod,
                //                 IndexType.SINGLE_PARTITION_WORD_INVIX ||
                //                           SINGLE_PARTITION_NGRAM_INVIX ||
                //                           LENGTH_PARTITIONED_WORD_INVIX ||
                //                           LENGTH_PARTITIONED_NGRAM_INVIX]
                IAccessMethod chosenAccessMethod = amEntry.getKey();
                Index chosenIndex = indexEntry.getKey();
                boolean isKeywordOrNgramIndexChosen = false;
                if (chosenIndex.getIndexType() == IndexType.LENGTH_PARTITIONED_WORD_INVIX
                        || chosenIndex.getIndexType() == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX
                        || chosenIndex.getIndexType() == IndexType.SINGLE_PARTITION_WORD_INVIX
                        || chosenIndex.getIndexType() == IndexType.SINGLE_PARTITION_NGRAM_INVIX)
                    isKeywordOrNgramIndexChosen = true;
                if ((chosenAccessMethod == BTreeAccessMethod.INSTANCE && chosenIndex.getIndexType() != IndexType.BTREE)
                        || (chosenAccessMethod == RTreeAccessMethod.INSTANCE
                                && chosenIndex.getIndexType() != IndexType.RTREE)
                        || (chosenAccessMethod == InvertedIndexAccessMethod.INSTANCE && !isKeywordOrNgramIndexChosen)) {
                    continue;
                }
                return new Pair<IAccessMethod, Index>(chosenAccessMethod, chosenIndex);
            }
        }
        return null;
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
        Iterator<Map.Entry<Index, List<Pair<Integer, Integer>>>> indexExprAndVarIt = analysisCtx.indexExprsAndVars
                .entrySet().iterator();
        // Used to keep track of matched expressions (added for prefix search)
        int numMatchedKeys = 0;
        ArrayList<Integer> matchedExpressions = new ArrayList<Integer>();
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
                    final IOptimizableFuncExpr optFuncExpr = analysisCtx.matchedFuncExprs.get(exprAndVarIdx.first);
                    // If expr is not optimizable by concrete index then remove
                    // expr and continue.
                    if (!accessMethod.exprIsOptimizable(index, optFuncExpr)) {
                        exprsAndVarIter.remove();
                        continue;
                    }
                    boolean typeMatch = true;
                    //Prune indexes based on field types
                    List<IAType> indexedTypes = new ArrayList<IAType>();
                    //retrieve types of expressions joined/selected with an indexed field
                    for (int j = 0; j < optFuncExpr.getNumLogicalVars(); j++)
                        if (j != exprAndVarIdx.second)
                            indexedTypes.add(optFuncExpr.getFieldType(j));

                    //add constants in case of select
                    if (indexedTypes.size() < 2 && optFuncExpr.getNumLogicalVars() == 1
                            && optFuncExpr.getNumConstantAtRuntimeExpr() > 0) {
                        indexedTypes.add((IAType) AqlExpressionTypeComputer.INSTANCE.getType(
                                optFuncExpr.getConstantAtRuntimeExpr(0), context.getMetadataProvider(),
                                typeEnvironment));
                    }

                    //infer type of logicalExpr based on index keyType
                    indexedTypes.add((IAType) AqlExpressionTypeComputer.INSTANCE.getType(
                            optFuncExpr.getLogicalExpr(exprAndVarIdx.second), null, new IVariableTypeEnvironment() {

                                @Override
                                public Object getVarType(LogicalVariable var) throws AlgebricksException {
                                    if (var.equals(optFuncExpr.getSourceVar(exprAndVarIdx.second)))
                                        return keyType;
                                    throw new IllegalArgumentException();
                                }

                                @Override
                                public Object getVarType(LogicalVariable var, List<LogicalVariable> nonNullVariables,
                                        List<List<LogicalVariable>> correlatedNullableVariableLists)
                                                throws AlgebricksException {
                                    if (var.equals(optFuncExpr.getSourceVar(exprAndVarIdx.second)))
                                        return keyType;
                                    throw new IllegalArgumentException();
                                }

                                @Override
                                public void setVarType(LogicalVariable var, Object type) {
                                    throw new IllegalArgumentException();
                                }

                                @Override
                                public Object getType(ILogicalExpression expr) throws AlgebricksException {
                                    return AqlExpressionTypeComputer.INSTANCE.getType(expr, null, this);
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

                    for (int j = 0; j < indexedTypes.size(); j++)
                        for (int k = j + 1; k < indexedTypes.size(); k++)
                            typeMatch &= isMatched(indexedTypes.get(j), indexedTypes.get(k), jaccardSimilarity);

                    // Check if any field name in the optFuncExpr matches.
                    if (optFuncExpr.findFieldName(keyField) != -1) {
                        foundKeyField = typeMatch
                                && optFuncExpr.getOperatorSubTree(exprAndVarIdx.second).hasDataSourceScan();
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
            analysisCtx.indexNumMatchedKeys.put(index, new Integer(numMatchedKeys));
        }
    }

    private boolean isMatched(IAType type1, IAType type2, boolean useListDomain) throws AlgebricksException {
        if (ATypeHierarchy.isSameTypeDomain(Index.getNonNullableType(type1).first.getTypeTag(),
                Index.getNonNullableType(type2).first.getTypeTag(), useListDomain))
            return true;
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
    protected boolean analyzeCondition(ILogicalExpression cond, List<AbstractLogicalOperator> assignsAndUnnests,
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs, IOptimizationContext context,
            IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) cond;
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();
        // Don't consider optimizing a disjunctive condition with an index (too
        // complicated for now).
        if (funcIdent == AlgebricksBuiltinFunctions.OR) {
            return false;
        }
        boolean found = analyzeFunctionExpr(funcExpr, assignsAndUnnests, analyzedAMs, context, typeEnvironment);
        for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
            ILogicalExpression argExpr = arg.getValue();
            if (argExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            AbstractFunctionCallExpression argFuncExpr = (AbstractFunctionCallExpression) argExpr;
            boolean matchFound = analyzeFunctionExpr(argFuncExpr, assignsAndUnnests, analyzedAMs, context,
                    typeEnvironment);
            found = found || matchFound;
        }
        return found;
    }

    /**
     * Finds applicable access methods for the given function expression based
     * on the function identifier, and an analysis of the function's arguments.
     * Updates the analyzedAMs accordingly.
     *
     * @throws AlgebricksException
     */
    protected boolean analyzeFunctionExpr(AbstractFunctionCallExpression funcExpr,
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
            boolean matchFound = accessMethod.analyzeFuncExprArgs(funcExpr, assignsAndUnnests, analysisCtx, context,
                    typeEnvironment);
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
            OptimizableOperatorSubTree matchedSubTree, AccessMethodAnalysisContext analysisCtx)
                    throws AlgebricksException {
        List<Index> indexCandidates = new ArrayList<Index>();
        // Add an index to the candidates if one of the indexed fields is
        // fieldName
        for (Index index : datasetIndexes) {
            // Need to also verify the index is pending no op
            if (index.getKeyFieldNames().contains(fieldName) && index.getPendingOp() == IMetadataEntity.PENDING_NO_OP) {
                indexCandidates.add(index);
                if (optFuncExpr.getFieldType(varIdx) == BuiltinType.ANULL
                        || optFuncExpr.getFieldType(varIdx) == BuiltinType.ANY)
                    optFuncExpr.setFieldType(varIdx,
                            index.getKeyFieldTypes().get(index.getKeyFieldNames().indexOf(fieldName)));
                analysisCtx.addIndexExpr(matchedSubTree.dataset, index, matchedFuncExprIndex, varIdx);
            }
        }
        // No index candidates for fieldName.
        if (indexCandidates.isEmpty()) {
            return false;
        }
        return true;
    }

    protected void fillAllIndexExprs(OptimizableOperatorSubTree subTree, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context) throws AlgebricksException {
        int optFuncExprIndex = 0;
        List<Index> datasetIndexes = new ArrayList<Index>();
        if (subTree.dataSourceType != DataSourceType.COLLECTION_SCAN)
            datasetIndexes = metadataProvider.getDatasetIndexes(subTree.dataset.getDataverseName(),
                    subTree.dataset.getDatasetName());
        for (IOptimizableFuncExpr optFuncExpr : analysisCtx.matchedFuncExprs) {
            // Try to match variables from optFuncExpr to assigns or unnests.
            for (int assignOrUnnestIndex = 0; assignOrUnnestIndex < subTree.assignsAndUnnests
                    .size(); assignOrUnnestIndex++) {
                AbstractLogicalOperator op = subTree.assignsAndUnnests.get(assignOrUnnestIndex);
                if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                    AssignOperator assignOp = (AssignOperator) op;
                    List<LogicalVariable> varList = assignOp.getVariables();
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
                        List<String> fieldName = getFieldNameFromSubTree(optFuncExpr, subTree, assignOrUnnestIndex,
                                varIndex, subTree.recordType, optVarIndex,
                                optFuncExpr.getFuncExpr().getArguments().get(optVarIndex).getValue());
                        if (fieldName == null) {
                            continue;
                        }
                        IAType fieldType = (IAType) context.getOutputTypeEnvironment(assignOp)
                                .getType(optFuncExpr.getLogicalExpr(optVarIndex));
                        // Set the fieldName in the corresponding matched
                        // function expression.
                        optFuncExpr.setFieldName(optVarIndex, fieldName);
                        optFuncExpr.setFieldType(optVarIndex, fieldType);

                        setTypeTag(context, subTree, optFuncExpr, optVarIndex);
                        if (subTree.hasDataSource()) {
                            fillIndexExprs(datasetIndexes, fieldName, fieldType, optFuncExpr, optFuncExprIndex,
                                    optVarIndex, subTree, analysisCtx);
                        }
                    }
                } else {
                    UnnestOperator unnestOp = (UnnestOperator) op;
                    LogicalVariable var = unnestOp.getVariable();
                    int funcVarIndex = optFuncExpr.findLogicalVar(var);
                    // No matching var in optFuncExpr.
                    if (funcVarIndex == -1) {
                        continue;
                    }
                    // At this point we have matched the optimizable func expr
                    // at optFuncExprIndex to an unnest variable.
                    // Remember matching subtree.
                    optFuncExpr.setOptimizableSubTree(funcVarIndex, subTree);
                    List<String> fieldName = null;
                    if (subTree.dataSourceType == DataSourceType.COLLECTION_SCAN) {
                        optFuncExpr.setLogicalExpr(funcVarIndex, new VariableReferenceExpression(var));
                    } else {
                        fieldName = getFieldNameFromSubTree(optFuncExpr, subTree, assignOrUnnestIndex, 0,
                                subTree.recordType, funcVarIndex,
                                optFuncExpr.getFuncExpr().getArguments().get(funcVarIndex).getValue());
                        if (fieldName == null) {
                            continue;
                        }
                    }
                    IAType fieldType = (IAType) context.getOutputTypeEnvironment(unnestOp)
                            .getType(optFuncExpr.getLogicalExpr(funcVarIndex));
                    // Set the fieldName in the corresponding matched function
                    // expression.
                    optFuncExpr.setFieldName(funcVarIndex, fieldName);
                    optFuncExpr.setFieldType(funcVarIndex, fieldType);

                    setTypeTag(context, subTree, optFuncExpr, funcVarIndex);
                    if (subTree.hasDataSource()) {
                        fillIndexExprs(datasetIndexes, fieldName, fieldType, optFuncExpr, optFuncExprIndex,
                                funcVarIndex, subTree, analysisCtx);
                    }
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
                additionalDsVarList = new ArrayList<LogicalVariable>();
                for (int i = 0; i < subTree.ixJoinOuterAdditionalDataSourceRefs.size(); i++) {
                    additionalDsVarList.addAll(subTree.getIxJoinOuterAdditionalDataSourceVariables(i));
                }

                matchVarsFromOptFuncExprToDataSourceScan(optFuncExpr, optFuncExprIndex, datasetIndexes,
                        additionalDsVarList, subTree, analysisCtx, context, true);

            }

            optFuncExprIndex++;
        }
    }

    private void matchVarsFromOptFuncExprToDataSourceScan(IOptimizableFuncExpr optFuncExpr, int optFuncExprIndex,
            List<Index> datasetIndexes, List<LogicalVariable> dsVarList, OptimizableOperatorSubTree subTree,
            AccessMethodAnalysisContext analysisCtx, IOptimizationContext context, boolean fromAdditionalDataSource)
                    throws AlgebricksException {
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

            if (!fromAdditionalDataSource) {
                fieldName = DatasetUtils.getPartitioningKeys(subTree.dataset).get(varIndex);
                fieldType = (IAType) context.getOutputTypeEnvironment(subTree.dataSourceRef.getValue()).getVarType(var);
            } else {
                fieldName = DatasetUtils.getPartitioningKeys(subTree.ixJoinOuterAdditionalDatasets.get(varIndex))
                        .get(varIndex);
                fieldType = (IAType) context
                        .getOutputTypeEnvironment(subTree.ixJoinOuterAdditionalDataSourceRefs.get(varIndex).getValue())
                        .getVarType(var);
            }
            // Set the fieldName in the corresponding matched function
            // expression, and remember matching subtree.
            optFuncExpr.setFieldName(funcVarIndex, fieldName);
            optFuncExpr.setOptimizableSubTree(funcVarIndex, subTree);
            optFuncExpr.setSourceVar(funcVarIndex, var);
            optFuncExpr.setLogicalExpr(funcVarIndex, new VariableReferenceExpression(var));
            setTypeTag(context, subTree, optFuncExpr, funcVarIndex);
            if (subTree.hasDataSourceScan()) {
                fillIndexExprs(datasetIndexes, fieldName, fieldType, optFuncExpr, optFuncExprIndex, funcVarIndex,
                        subTree, analysisCtx);
            }
        }
    }

    private void setTypeTag(IOptimizationContext context, OptimizableOperatorSubTree subTree,
            IOptimizableFuncExpr optFuncExpr, int funcVarIndex) throws AlgebricksException {
        // Set the typeTag if the type is not null
        IAType type = (IAType) context.getOutputTypeEnvironment(subTree.root)
                .getVarType(optFuncExpr.getLogicalVar(funcVarIndex));
        optFuncExpr.setFieldType(funcVarIndex, type);
    }

    /**
     * Returns the field name corresponding to the assigned variable at
     * varIndex. Returns null if the expr at varIndex does not yield to a field
     * access function after following a set of allowed functions.
     *
     * @throws AlgebricksException
     */
    protected List<String> getFieldNameFromSubTree(IOptimizableFuncExpr optFuncExpr, OptimizableOperatorSubTree subTree,
            int opIndex, int assignVarIndex, ARecordType recordType, int funcVarIndex,
            ILogicalExpression parentFuncExpr) throws AlgebricksException {
        // Get expression corresponding to opVar at varIndex.
        AbstractLogicalExpression expr = null;
        AbstractFunctionCallExpression childFuncExpr = null;
        AbstractLogicalOperator op = subTree.assignsAndUnnests.get(opIndex);
        if (op.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator assignOp = (AssignOperator) op;
            expr = (AbstractLogicalExpression) assignOp.getExpressions().get(assignVarIndex).getValue();
            childFuncExpr = (AbstractFunctionCallExpression) expr;
        } else {
            UnnestOperator unnestOp = (UnnestOperator) op;
            expr = (AbstractLogicalExpression) unnestOp.getExpressionRef().getValue();
            if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                return null;
            }
            childFuncExpr = (AbstractFunctionCallExpression) expr;
            if (childFuncExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.SCAN_COLLECTION) {
                return null;
            }
            expr = (AbstractLogicalExpression) childFuncExpr.getArguments().get(0).getValue();
        }
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();

        boolean isByName = false;
        boolean isFieldAccess = false;
        String fieldName = null;
        List<String> nestedAccessFieldName = null;
        int fieldIndex = -1;
        if (funcIdent == AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME) {
            ILogicalExpression nameArg = funcExpr.getArguments().get(1).getValue();
            if (nameArg.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                return null;
            }
            ConstantExpression constExpr = (ConstantExpression) nameArg;
            fieldName = ((AString) ((AsterixConstantValue) constExpr.getValue()).getObject()).getStringValue();
            isFieldAccess = true;
            isByName = true;
        } else if (funcIdent == AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX) {
            ILogicalExpression idxArg = funcExpr.getArguments().get(1).getValue();
            if (idxArg.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                return null;
            }
            ConstantExpression constExpr = (ConstantExpression) idxArg;
            fieldIndex = ((AInt32) ((AsterixConstantValue) constExpr.getValue()).getObject()).getIntegerValue();
            isFieldAccess = true;
        } else if (funcIdent == AsterixBuiltinFunctions.FIELD_ACCESS_NESTED) {
            ILogicalExpression nameArg = funcExpr.getArguments().get(1).getValue();
            if (nameArg.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                return null;
            }
            ConstantExpression constExpr = (ConstantExpression) nameArg;
            AOrderedList orderedNestedFieldName = (AOrderedList) ((AsterixConstantValue) constExpr.getValue())
                    .getObject();
            nestedAccessFieldName = new ArrayList<String>();
            for (int i = 0; i < orderedNestedFieldName.size(); i++) {
                nestedAccessFieldName.add(((AString) orderedNestedFieldName.getItem(i)).getStringValue());
            }
            isFieldAccess = true;
            isByName = true;
        }
        if (isFieldAccess) {
            optFuncExpr.setLogicalExpr(funcVarIndex, parentFuncExpr);
            int[] assignAndExpressionIndexes = null;

            //go forward through nested assigns until you find the relevant one
            for (int i = opIndex + 1; i < subTree.assignsAndUnnests.size(); i++) {
                AbstractLogicalOperator subOp = subTree.assignsAndUnnests.get(i);
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
                    ArrayList<LogicalVariable> parentVars = new ArrayList<LogicalVariable>();
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
                        parentFuncExpr);

                if (parentFieldNames == null) {
                    //Nested assign was not a field access.
                    //We will not use index
                    return null;
                }

                if (!isByName) {
                    try {
                        fieldName = ((ARecordType) recordType.getSubFieldType(parentFieldNames))
                                .getFieldNames()[fieldIndex];
                    } catch (IOException e) {
                        throw new AlgebricksException(e);
                    }
                }
                optFuncExpr.setSourceVar(funcVarIndex, ((AssignOperator) op).getVariables().get(assignVarIndex));
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

            optFuncExpr.setSourceVar(funcVarIndex, ((AssignOperator) op).getVariables().get(assignVarIndex));
            //no nested assign, we are at the lowest level.
            if (isByName) {
                if (nestedAccessFieldName != null) {
                    return nestedAccessFieldName;
                }
                return new ArrayList<String>(Arrays.asList(fieldName));
            }
            return new ArrayList<String>(Arrays.asList(recordType.getFieldNames()[fieldIndex]));

        }

        if (funcIdent != AsterixBuiltinFunctions.WORD_TOKENS && funcIdent != AsterixBuiltinFunctions.GRAM_TOKENS
                && funcIdent != AsterixBuiltinFunctions.SUBSTRING
                && funcIdent != AsterixBuiltinFunctions.SUBSTRING_BEFORE
                && funcIdent != AsterixBuiltinFunctions.SUBSTRING_AFTER
                && funcIdent != AsterixBuiltinFunctions.CREATE_POLYGON
                && funcIdent != AsterixBuiltinFunctions.CREATE_MBR
                && funcIdent != AsterixBuiltinFunctions.CREATE_RECTANGLE
                && funcIdent != AsterixBuiltinFunctions.CREATE_CIRCLE
                && funcIdent != AsterixBuiltinFunctions.CREATE_LINE
                && funcIdent != AsterixBuiltinFunctions.CREATE_POINT) {
            return null;
        }
        // We use a part of the field in edit distance computation
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK) {
            optFuncExpr.setPartialField(true);
        }
        // We expect the function's argument to be a variable, otherwise we
        // cannot apply an index.
        ILogicalExpression argExpr = funcExpr.getArguments().get(0).getValue();
        if (argExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return null;
        }
        LogicalVariable curVar = ((VariableReferenceExpression) argExpr).getVariableReference();
        // We look for the assign or unnest operator that produces curVar below
        // the current operator
        for (int assignOrUnnestIndex = opIndex + 1; assignOrUnnestIndex < subTree.assignsAndUnnests
                .size(); assignOrUnnestIndex++) {
            AbstractLogicalOperator curOp = subTree.assignsAndUnnests.get(assignOrUnnestIndex);
            if (curOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                AssignOperator assignOp = (AssignOperator) curOp;
                List<LogicalVariable> varList = assignOp.getVariables();
                for (int varIndex = 0; varIndex < varList.size(); varIndex++) {
                    LogicalVariable var = varList.get(varIndex);
                    if (var.equals(curVar)) {
                        optFuncExpr.setSourceVar(funcVarIndex, var);
                        return getFieldNameFromSubTree(optFuncExpr, subTree, assignOrUnnestIndex, varIndex, recordType,
                                funcVarIndex, childFuncExpr);
                    }
                }
            } else {
                UnnestOperator unnestOp = (UnnestOperator) curOp;
                LogicalVariable var = unnestOp.getVariable();
                if (var.equals(curVar)) {
                    getFieldNameFromSubTree(optFuncExpr, subTree, assignOrUnnestIndex, 0, recordType, funcVarIndex,
                            childFuncExpr);
                }
            }
        }
        return null;
    }
}