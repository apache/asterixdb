package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;

/**
 * Interface that an access method should implement to work with the rewrite
 * rules to apply it for join and/or selection queries. This interface provides
 * methods for analyzing a select/join condition, and for rewriting the plan
 * with a given index.
 */
public interface IAccessMethod {
    
    /**
     * @return A list of function identifiers that are optimizable by this
     *         access method.
     */
    public List<FunctionIdentifier> getOptimizableFunctions();
    
    /**
     * Analyzes the arguments of a given optimizable funcExpr to see if this
     * access method is applicable (e.g., one arg is a constant and one is a
     * var). We assume that the funcExpr has already been determined to be
     * optimizable by this access method based on its function identifier. If
     * funcExpr has been found to be optimizable, this method adds an
     * OptimizableFunction to analysisCtx.matchedFuncExprs for further analysis.
     * 
     * @return true if funcExpr is optimizable by this access method, false
     *         otherwise
     */
    public boolean analyzeFuncExprArgs(AbstractFunctionCallExpression funcExpr, List<AssignOperator> assigns, AccessMethodAnalysisContext analysisCtx);
    
    /**
     * Indicates whether all index expressions must be matched in order for this
     * index to be applicable.
     * 
     * @return boolean
     */
    public boolean matchAllIndexExprs();

    /**
     * Indicates whether this index is applicable if only a prefix of the index
     * expressions are matched.
     * 
     * @return boolean
     */
    public boolean matchPrefixIndexExprs();
    
    /**
     * Applies the plan transformation to use chosenIndex to optimize a selection query.
     */
    public boolean applySelectPlanTransformation(Mutable<ILogicalOperator> selectRef, OptimizableOperatorSubTree subTree,            
            AqlCompiledIndexDecl chosenIndex, AccessMethodAnalysisContext analysisCtx, IOptimizationContext context)
            throws AlgebricksException;
    
    /**
     * Applies the plan transformation to use chosenIndex to optimize a join query.
     */
    public boolean applyJoinPlanTransformation(Mutable<ILogicalOperator> joinRef, OptimizableOperatorSubTree leftSubTree, OptimizableOperatorSubTree rightSubTree,
            AqlCompiledIndexDecl chosenIndex, AccessMethodAnalysisContext analysisCtx, IOptimizationContext context)
            throws AlgebricksException;
    
    /**
     * Analyzes expr to see whether it is optimizable by the given concrete index.
     */
    public boolean exprIsOptimizable(AqlCompiledIndexDecl index, IOptimizableFuncExpr optFuncExpr);
}
