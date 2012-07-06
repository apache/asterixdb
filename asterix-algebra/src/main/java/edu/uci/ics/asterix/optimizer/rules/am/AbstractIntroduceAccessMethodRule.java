package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Class that embodies the commonalities between rewrite rules for access methods.
 */
public abstract class AbstractIntroduceAccessMethodRule implements IAlgebraicRewriteRule {
    
    public abstract Map<FunctionIdentifier, List<IAccessMethod>> getAccessMethods();
    
    protected static void registerAccessMethod(IAccessMethod accessMethod, Map<FunctionIdentifier, List<IAccessMethod>> accessMethods) {
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
    
    protected void fillSubTreeIndexExprs(OptimizableOperatorSubTree subTree, Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs) {
        // The assign may be null if there is only a filter on the primary index key.
        // Match variables from lowest assign which comes directly after the dataset scan.
        List<LogicalVariable> varList = (!subTree.assigns.isEmpty()) ? subTree.assigns.get(subTree.assigns.size() - 1).getVariables() : subTree.dataSourceScan.getVariables();
        Iterator<Map.Entry<IAccessMethod, AccessMethodAnalysisContext>> amIt = analyzedAMs.entrySet().iterator();
        // Check applicability of indexes by access method type.
        while (amIt.hasNext()) {
            Map.Entry<IAccessMethod, AccessMethodAnalysisContext> entry = amIt.next();
            AccessMethodAnalysisContext amCtx = entry.getValue();
            // For the current access method type, map variables from the assign op to applicable indexes.
            fillAllIndexExprs(varList, subTree, amCtx);
        }
    }

    protected void pruneIndexCandidates(Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs) {
        Iterator<Map.Entry<IAccessMethod, AccessMethodAnalysisContext>> amIt = analyzedAMs.entrySet().iterator();
        // Check applicability of indexes by access method type.
        while (amIt.hasNext()) {
            Map.Entry<IAccessMethod, AccessMethodAnalysisContext> entry = amIt.next();
            AccessMethodAnalysisContext amCtx = entry.getValue();
            pruneIndexCandidates(entry.getKey(), amCtx);
            // Remove access methods for which there are definitely no applicable indexes.
            if (amCtx.indexExprs.isEmpty()) {
                amIt.remove();
            }
        }
    }
    
    /**
     * Simply picks the first index that it finds.
     * TODO: Improve this decision process by making it more systematic.
     */
    protected Pair<IAccessMethod, AqlCompiledIndexDecl> chooseIndex(
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs) {
        Iterator<Map.Entry<IAccessMethod, AccessMethodAnalysisContext>> amIt = analyzedAMs.entrySet().iterator();
        while (amIt.hasNext()) {
            Map.Entry<IAccessMethod, AccessMethodAnalysisContext> amEntry = amIt.next();
            AccessMethodAnalysisContext analysisCtx = amEntry.getValue();
            Iterator<Map.Entry<AqlCompiledIndexDecl, List<Integer>>> indexIt = analysisCtx.indexExprs.entrySet().iterator();
            if (indexIt.hasNext()) {
                Map.Entry<AqlCompiledIndexDecl, List<Integer>> indexEntry = indexIt.next();
                return new Pair<IAccessMethod, AqlCompiledIndexDecl>(amEntry.getKey(), indexEntry.getKey());
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
     */
    public void pruneIndexCandidates(IAccessMethod accessMethod, AccessMethodAnalysisContext analysisCtx) {
        Iterator<Map.Entry<AqlCompiledIndexDecl, List<Integer>>> it = analysisCtx.indexExprs.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<AqlCompiledIndexDecl, List<Integer>> entry = it.next();
            AqlCompiledIndexDecl index = entry.getKey();
            Iterator<Integer> exprsIter = entry.getValue().iterator();
            boolean allUsed = true;
            int lastFieldMatched = -1;
            for (int i = 0; i < index.getFieldExprs().size(); i++) {
                String keyField = index.getFieldExprs().get(i);
                boolean foundKeyField = false;
                while (exprsIter.hasNext()) {
                    Integer ix = exprsIter.next();
                    IOptimizableFuncExpr optFuncExpr = analysisCtx.matchedFuncExprs.get(ix);
                    // If expr is not optimizable by concrete index then remove expr and continue.
                    if (!accessMethod.exprIsOptimizable(index, optFuncExpr)) {
                        exprsIter.remove();
                        continue;
                    }
                    // Check if any field name in the optFuncExpr matches.
                    if (optFuncExpr.findFieldName(keyField) != -1) {
                        foundKeyField = true;
                        if (lastFieldMatched == i - 1) {
                            lastFieldMatched = i;
                        }
                        break;
                    }
                }
                if (!foundKeyField) {
                    allUsed = false;
                    break;
                }                
            }
            // If the access method requires all exprs to be matched but they are not, remove this candidate.
            if (!allUsed && accessMethod.matchAllIndexExprs()) {
                it.remove();
                return;
            }
            // A prefix of the index exprs may have been matched.
            if (lastFieldMatched < 0 && accessMethod.matchPrefixIndexExprs()) {
                it.remove();
                return;
            }
        }
    }
    
    /**
     * Analyzes the given selection condition, filling analyzedAMs with applicable access method types.
     * At this point we are not yet consulting the metadata whether an actual index exists or not.
     */
    protected boolean analyzeCondition(ILogicalExpression cond, List<AssignOperator> assigns, Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs) {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) cond;
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();
        // Don't consider optimizing a disjunctive condition with an index (too complicated for now).
        if (funcIdent == AlgebricksBuiltinFunctions.OR) {
            return false;
        }
        boolean found = analyzeFunctionExpr(funcExpr, assigns, analyzedAMs);        
        for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
            ILogicalExpression argExpr = arg.getValue();
            if (argExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            AbstractFunctionCallExpression argFuncExpr = (AbstractFunctionCallExpression) argExpr;
            boolean matchFound = analyzeFunctionExpr(argFuncExpr, assigns, analyzedAMs); 
            found = found || matchFound;
        }
        return found;
    }
    
    /**
     * Finds applicable access methods for the given function expression based
     * on the function identifier, and an analysis of the function's arguments.
     * Updates the analyzedAMs accordingly.
     */
    protected boolean analyzeFunctionExpr(AbstractFunctionCallExpression funcExpr, List<AssignOperator> assigns, Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs) {
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();
        if (funcIdent == AlgebricksBuiltinFunctions.AND) {
            return false;
        }
        // Retrieves the list of access methods that are relevant based on the funcIdent.
        List<IAccessMethod> relevantAMs = getAccessMethods().get(funcIdent);
        if (relevantAMs == null) {
            return false;
        }
        boolean atLeastOneMatchFound = false;
        // Place holder for a new analysis context in case we need one.
        AccessMethodAnalysisContext newAnalysisCtx = new AccessMethodAnalysisContext();
        for(IAccessMethod accessMethod : relevantAMs) {
            AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(accessMethod);
            // Use the current place holder.
            if (analysisCtx == null) {
                analysisCtx = newAnalysisCtx;
            }
            // Analyzes the funcExpr's arguments to see if the accessMethod is truly applicable.
            boolean matchFound = accessMethod.analyzeFuncExprArgs(funcExpr, assigns, analysisCtx);
            if (matchFound) {
                // If we've used the current new context placeholder, replace it with a new one.
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
     * Finds secondary indexes whose keys include fieldName, and adds a mapping in analysisCtx.indexEsprs 
     * from that index to the a corresponding optimizable function expression.
     * 
     * @return true if a candidate index was added to foundIndexExprs, false
     *         otherwise
     */
    protected boolean fillIndexExprs(String fieldName, int matchedFuncExprIndex,
            AqlCompiledDatasetDecl datasetDecl, AccessMethodAnalysisContext analysisCtx) {
        AqlCompiledIndexDecl primaryIndexDecl = DatasetUtils.getPrimaryIndex(datasetDecl);
        List<String> primaryIndexFields = primaryIndexDecl.getFieldExprs();     
        List<AqlCompiledIndexDecl> indexCandidates = DatasetUtils.findSecondaryIndexesByOneOfTheKeys(datasetDecl, fieldName);
        // Check whether the primary index is a candidate. If so, add it to the list.
        if (primaryIndexFields.contains(fieldName)) {
            if (indexCandidates == null) {
                indexCandidates = new ArrayList<AqlCompiledIndexDecl>(1);
            }
            indexCandidates.add(primaryIndexDecl);
        }
        // No index candidates for fieldName.
        if (indexCandidates == null) {
            return false;
        }
        // Go through the candidates and fill indexExprs.
        for (AqlCompiledIndexDecl index : indexCandidates) {
            analysisCtx.addIndexExpr(datasetDecl, index, matchedFuncExprIndex);
        }
        return true;
    }

    protected void fillAllIndexExprs(List<LogicalVariable> varList, OptimizableOperatorSubTree subTree, AccessMethodAnalysisContext analysisCtx) {
        for (int optFuncExprIndex = 0; optFuncExprIndex < analysisCtx.matchedFuncExprs.size(); optFuncExprIndex++) {
            for (int varIndex = 0; varIndex < varList.size(); varIndex++) {
                LogicalVariable var = varList.get(varIndex);
                IOptimizableFuncExpr optFuncExpr = analysisCtx.matchedFuncExprs.get(optFuncExprIndex);
                int funcVarIndex = optFuncExpr.findLogicalVar(var);
                // No matching var in optFuncExpr.
                if (funcVarIndex == -1) {
                    continue;
                }
                // At this point we have matched the optimizable func expr at optFuncExprIndex to an assigned variable.
                String fieldName = null;
                if (!subTree.assigns.isEmpty()) {
                    // Get the fieldName corresponding to the assigned variable at varIndex
                    // from the assign operator right above the datasource scan.
                    // If the expr at varIndex is not a fieldAccess we get back null.
                    fieldName = getFieldNameOfFieldAccess(subTree.assigns.get(subTree.assigns.size() - 1), subTree.recordType, varIndex);
                    if (fieldName == null) {
                        continue;
                    }
                } else {
                    // We don't have an assign, only a datasource scan.
                    // The last var. is the record itself, so skip it.
                    if (varIndex >= varList.size() - 1) {
                        break;
                    }
                    // The variable value is one of the partitioning fields.
                    fieldName = DatasetUtils.getPartitioningExpressions(subTree.datasetDecl).get(varIndex);
                }
                // Set the fieldName in the corresponding matched function expression, and remember matching subtree.
                optFuncExpr.setFieldName(funcVarIndex, fieldName);
                optFuncExpr.setOptimizableSubTree(funcVarIndex, subTree);
                fillIndexExprs(fieldName, optFuncExprIndex, subTree.datasetDecl, analysisCtx);
            }
        }
    }
    
    /**
     * Returns the field name corresponding to the assigned variable at varIndex.
     * Returns null if the expr at varIndex is not a field access function.
     */
    protected String getFieldNameOfFieldAccess(AssignOperator assign, ARecordType recordType, int varIndex) {
        // Get expression corresponding to var at varIndex.
        AbstractLogicalExpression assignExpr = (AbstractLogicalExpression) assign.getExpressions()
                .get(varIndex).getValue();
        if (assignExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        // Analyze the assign op to get the field name
        // corresponding to the field being assigned at varIndex.
        AbstractFunctionCallExpression assignFuncExpr = (AbstractFunctionCallExpression) assignExpr;
        FunctionIdentifier assignFuncIdent = assignFuncExpr.getFunctionIdentifier();
        if (assignFuncIdent == AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME) {
            ILogicalExpression nameArg = assignFuncExpr.getArguments().get(1).getValue();
            if (nameArg.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                return null;
            }
            ConstantExpression constExpr = (ConstantExpression) nameArg;
            return ((AString) ((AsterixConstantValue) constExpr.getValue()).getObject()).getStringValue();
        } else if (assignFuncIdent == AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX) {
            ILogicalExpression idxArg = assignFuncExpr.getArguments().get(1).getValue();
            if (idxArg.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                return null;
            }
            ConstantExpression constExpr = (ConstantExpression) idxArg;
            int fieldIndex = ((AInt32) ((AsterixConstantValue) constExpr.getValue()).getObject()).getIntegerValue();
            return recordType.getFieldNames()[fieldIndex];
        }
        return null;
    }
}
