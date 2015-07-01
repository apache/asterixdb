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

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
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
    protected final LogicalVariable[] sourceVars;
    protected final ILogicalExpression[] logicalExprs;
    protected final List<List<String>> fieldNames;
    protected final IAType[] fieldTypes;
    protected final OptimizableOperatorSubTree[] subTrees;
    protected final IAlgebricksConstantValue[] constantVals;
    protected boolean partialField;

    public OptimizableFuncExpr(AbstractFunctionCallExpression funcExpr, LogicalVariable[] logicalVars,
            IAlgebricksConstantValue[] constantVals) {
        this.funcExpr = funcExpr;
        this.logicalVars = logicalVars;
        this.sourceVars = new LogicalVariable[logicalVars.length];
        this.logicalExprs = new ILogicalExpression[logicalVars.length];
        this.constantVals = constantVals;
        this.fieldNames = new ArrayList<List<String>>();
        for (int i = 0; i < logicalVars.length; i++) {
            fieldNames.add(new ArrayList<String>());
        }
        this.fieldTypes = new IAType[logicalVars.length];
        this.subTrees = new OptimizableOperatorSubTree[logicalVars.length];

        if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CONTAINS) {
            this.partialField = true;
        } else {
            this.partialField = false;
        }
    }

    // Special, more convenient c'tor for simple binary functions.
    public OptimizableFuncExpr(AbstractFunctionCallExpression funcExpr, LogicalVariable logicalVar,
            IAlgebricksConstantValue constantVal) {
        this.funcExpr = funcExpr;
        this.logicalVars = new LogicalVariable[] { logicalVar };
        this.sourceVars = new LogicalVariable[1];
        this.logicalExprs = new ILogicalExpression[1];
        this.constantVals = new IAlgebricksConstantValue[] { constantVal };
        this.fieldNames = new ArrayList<List<String>>();
        for (int i = 0; i < logicalVars.length; i++) {
            fieldNames.add(new ArrayList<String>());
        }
        this.fieldTypes = new IAType[logicalVars.length];
        this.subTrees = new OptimizableOperatorSubTree[logicalVars.length];
        if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CONTAINS) {
            this.partialField = true;
        } else {
            this.partialField = false;
        }
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
    public void setLogicalExpr(int index, ILogicalExpression logExpr) {
        logicalExprs[index] = logExpr;
    }

    @Override
    public ILogicalExpression getLogicalExpr(int index) {
        return logicalExprs[index];
    }

    @Override
    public void setFieldName(int index, List<String> fieldName) {
        fieldNames.set(index, fieldName);
    }

    @Override
    public List<String> getFieldName(int index) {
        return fieldNames.get(index);
    }

    @Override
    public void setFieldType(int index, IAType fieldType) {
        fieldTypes[index] = fieldType;
    }

    @Override
    public IAType getFieldType(int index) {
        return fieldTypes[index];
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
    public int findFieldName(List<String> fieldName) {
        for (int i = 0; i < fieldNames.size(); i++) {
            if (fieldName.equals(fieldNames.get(i))) {
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

    @Override
    public void setPartialField(boolean partialField) {
        this.partialField = partialField;
    }

    @Override
    public boolean containsPartialField() {
        return partialField;
    }

    @Override
    public int hashCode() {
        return funcExpr.hashCode();

    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof OptimizableFuncExpr))
            return false;
        else
            return funcExpr.equals(o);

    }

    public void setSourceVar(int index, LogicalVariable var) {
        sourceVars[index] = var;
    }

    @Override
    public LogicalVariable getSourceVar(int index) {
        return sourceVars[index];
    }

}
