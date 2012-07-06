package edu.uci.ics.asterix.optimizer.rules.am;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;

/**
 * Describes a function expression that is optimizable by an access method.
 * Provides convenient methods for accessing arguments (constants, variables)
 * and metadata of such a function.
 */
public interface IOptimizableFuncExpr {
    public AbstractFunctionCallExpression getFuncExpr();
    public int getNumLogicalVars();
    public int getNumConstantVals();
    public LogicalVariable getLogicalVar(int index);
    public void setFieldName(int index, String fieldName);
    public String getFieldName(int index);
    public void setOptimizableSubTree(int index, OptimizableOperatorSubTree subTree);
    public OptimizableOperatorSubTree getOperatorSubTree(int index);
    public IAlgebricksConstantValue getConstantVal(int index);
    
    public int findLogicalVar(LogicalVariable var);
    public int findFieldName(String fieldName);
}
