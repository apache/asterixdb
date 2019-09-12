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
import java.util.List;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

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
    protected final int[] fieldSources;
    protected final IAType[] fieldTypes;
    protected final OptimizableOperatorSubTree[] subTrees;
    protected final ILogicalExpression[] constantExpressions;
    protected final IAType[] constantExpressionTypes;
    protected boolean partialField;

    public OptimizableFuncExpr(AbstractFunctionCallExpression funcExpr, LogicalVariable[] logicalVars,
            ILogicalExpression[] constantExpressions, IAType[] constantExpressionTypes) {
        this.funcExpr = funcExpr;
        this.logicalVars = logicalVars;
        this.sourceVars = new LogicalVariable[logicalVars.length];
        this.logicalExprs = new ILogicalExpression[logicalVars.length];
        this.constantExpressionTypes = constantExpressionTypes;
        this.constantExpressions = constantExpressions;
        this.fieldSources = new int[logicalVars.length];
        this.fieldNames = new ArrayList<List<String>>();
        for (int i = 0; i < logicalVars.length; i++) {
            fieldNames.add(new ArrayList<String>());
        }
        this.fieldTypes = new IAType[logicalVars.length];
        this.subTrees = new OptimizableOperatorSubTree[logicalVars.length];

        if (funcExpr.getFunctionIdentifier() == BuiltinFunctions.EDIT_DISTANCE_CONTAINS) {
            this.partialField = true;
        } else {
            this.partialField = false;
        }
    }

    // Special, more convenient c'tor for simple binary functions.
    public OptimizableFuncExpr(AbstractFunctionCallExpression funcExpr, LogicalVariable logicalVar,
            ILogicalExpression constantExpression, IAType constantExpressionType) {
        this.funcExpr = funcExpr;
        this.logicalVars = new LogicalVariable[] { logicalVar };
        this.sourceVars = new LogicalVariable[1];
        this.logicalExprs = new ILogicalExpression[1];
        this.constantExpressions = new ILogicalExpression[] { constantExpression };
        this.constantExpressionTypes = new IAType[] { constantExpressionType };
        this.fieldSources = new int[logicalVars.length];
        this.fieldNames = new ArrayList<List<String>>();
        for (int i = 0; i < logicalVars.length; i++) {
            fieldNames.add(new ArrayList<String>());
        }
        this.fieldTypes = new IAType[logicalVars.length];
        this.subTrees = new OptimizableOperatorSubTree[logicalVars.length];
        if (funcExpr.getFunctionIdentifier() == BuiltinFunctions.EDIT_DISTANCE_CONTAINS) {
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
    public int getNumConstantExpr() {
        return constantExpressions.length;
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
    public void setFieldName(int index, List<String> fieldName, int fieldSource) {
        fieldNames.set(index, fieldName);
        fieldSources[index] = fieldSource;
    }

    @Override
    public List<String> getFieldName(int index) {
        return fieldNames.get(index);
    }

    @Override
    public int getFieldSource(int index) {
        return fieldSources[index];
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
    public ILogicalExpression getConstantExpr(int index) {
        return constantExpressions[index];
    }

    @Override
    public ILogicalExpression[] getConstantExpressions() {
        return constantExpressions;
    }

    @Override
    public void setConstType(int index, IAType fieldType) {
        constantExpressionTypes[index] = fieldType;
    }

    @Override
    public IAType getConstantType(int index) {
        return constantExpressionTypes[index];
    }

    @Override
    public void setConstantExpr(int index, ILogicalExpression expr) {
        constantExpressions[index] = expr;
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
