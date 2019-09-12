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

import java.util.List;

import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

/**
 * Describes a function expression that is optimizable by an access method.
 * Provides convenient methods for accessing arguments (constants, variables)
 * and metadata of such a function.
 */
public interface IOptimizableFuncExpr {
    public AbstractFunctionCallExpression getFuncExpr();

    public int getNumLogicalVars();

    public int getNumConstantExpr();

    public LogicalVariable getLogicalVar(int index);

    public void setLogicalExpr(int index, ILogicalExpression logExpr);

    public ILogicalExpression getLogicalExpr(int index);

    public void setFieldName(int index, List<String> fieldName, int fieldSource);

    public List<String> getFieldName(int index);

    public int getFieldSource(int index);

    public void setFieldType(int index, IAType fieldName);

    public IAType getFieldType(int index);

    public void setOptimizableSubTree(int index, OptimizableOperatorSubTree subTree);

    public OptimizableOperatorSubTree getOperatorSubTree(int index);

    public ILogicalExpression getConstantExpr(int index);

    public int findLogicalVar(LogicalVariable var);

    public int findFieldName(List<String> fieldName);

    public void substituteVar(LogicalVariable original, LogicalVariable substitution);

    public void setPartialField(boolean partialField);

    public boolean containsPartialField();

    public void setSourceVar(int index, LogicalVariable var);

    public LogicalVariable getSourceVar(int index);

    void setConstType(int index, IAType fieldType);

    IAType getConstantType(int index);

    void setConstantExpr(int index, ILogicalExpression expr);

    ILogicalExpression[] getConstantExpressions();
}
