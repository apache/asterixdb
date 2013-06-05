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

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;

/**
 * General-purpose implementation of IOptimizableFuncExpr that supports any
 * number of constant args, variable args and field names.
 */
public class OptimizableFuncExpr implements IOptimizableFuncExpr {
	protected final AbstractFunctionCallExpression funcExpr;
    protected final LogicalVariable[] logicalVars;
    protected final String[] fieldNames;
    protected final OptimizableOperatorSubTree[] subTrees;
    protected final IAlgebricksConstantValue[] constantVals;
    
    public OptimizableFuncExpr(AbstractFunctionCallExpression funcExpr, LogicalVariable[] logicalVars, IAlgebricksConstantValue[] constantVals) {
    	this.funcExpr = funcExpr;
    	this.logicalVars = logicalVars;
    	this.constantVals = constantVals;
    	this.fieldNames = new String[logicalVars.length];
    	this.subTrees = new OptimizableOperatorSubTree[logicalVars.length];
    }
    
    // Special, more convenient c'tor for simple binary functions.
    public OptimizableFuncExpr(AbstractFunctionCallExpression funcExpr, LogicalVariable logicalVar, IAlgebricksConstantValue constantVal) {
    	this.funcExpr = funcExpr;
    	this.logicalVars = new LogicalVariable[] { logicalVar };
    	this.constantVals = new IAlgebricksConstantValue[] { constantVal };
    	this.fieldNames = new String[logicalVars.length];
    	this.subTrees = new OptimizableOperatorSubTree[logicalVars.length];
    }
    
	@Override
	public AbstractFunctionCallExpression getFuncExpr() {
		return funcExpr;
	}
	
	@Override
	public int getNumLogicalVars() {
		return logicalVars.length;
	}
	
	@Override
	public int getNumConstantVals() {
		return constantVals.length;
	}
	
	@Override	
	public LogicalVariable getLogicalVar(int index) {
		return logicalVars[index];
	}
	
	@Override
	public void setFieldName(int index, String fieldName) {
		fieldNames[index] = fieldName;
	}
	
	@Override
	public String getFieldName(int index) {
		return fieldNames[index];
	}
	
	@Override
	public IAlgebricksConstantValue getConstantVal(int index) {
		return constantVals[index];
	}

    @Override
    public int findLogicalVar(LogicalVariable var) {
        for (int i = 0; i < logicalVars.length; i++) {
            if (var == logicalVars[i]) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public int findFieldName(String fieldName) {
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldName.equals(fieldNames[i])) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public void setOptimizableSubTree(int index, OptimizableOperatorSubTree subTree) {
        subTrees[index] = subTree;
    }

    @Override
    public OptimizableOperatorSubTree getOperatorSubTree(int index) {
        return subTrees[index];
    }

    @Override
    public void substituteVar(LogicalVariable original, LogicalVariable substitution) {
        if (logicalVars != null) {
            for (int i = 0; i < logicalVars.length; i++) {
                if (logicalVars[i] == original) {
                    logicalVars[i] = substitution;
                    break;
                }
            }
        }
    }
}
