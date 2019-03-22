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
package org.apache.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator.Kind;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class TokenizeOperator extends AbstractLogicalOperator {

    private final IDataSourceIndex<?, ?> dataSourceIndex;
    private final List<Mutable<ILogicalExpression>> primaryKeyExprs;
    private final List<Mutable<ILogicalExpression>> secondaryKeyExprs;
    // logical variables for token, and number of token
    private final List<LogicalVariable> tokenizeVars;
    private final Mutable<ILogicalExpression> filterExpr;
    private final Kind operation;
    private final boolean bulkload;
    private final boolean isPartitioned;
    // contains the type for each variable in the tokenizeVars
    private final List<Object> tokenizeVarTypes;
    private List<Mutable<ILogicalExpression>> additionalFilteringExpressions;

    public TokenizeOperator(IDataSourceIndex<?, ?> dataSourceIndex, List<Mutable<ILogicalExpression>> primaryKeyExprs,
            List<Mutable<ILogicalExpression>> secondaryKeyExprs, List<LogicalVariable> tokenizeVars,
            Mutable<ILogicalExpression> filterExpr, Kind operation, boolean bulkload, boolean isPartitioned,
            List<Object> tokenizeVarTypes) {
        this.dataSourceIndex = dataSourceIndex;
        this.primaryKeyExprs = primaryKeyExprs;
        this.secondaryKeyExprs = secondaryKeyExprs;
        this.tokenizeVars = tokenizeVars;
        this.filterExpr = filterExpr;
        this.operation = operation;
        this.bulkload = bulkload;
        this.isPartitioned = isPartitioned;
        this.tokenizeVarTypes = tokenizeVarTypes;
    }

    @Override
    public void recomputeSchema() throws AlgebricksException {
        schema = new ArrayList<LogicalVariable>();
        schema.addAll(inputs.get(0).getValue().getSchema());
        schema.addAll(tokenizeVars);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        boolean b = false;
        for (int i = 0; i < primaryKeyExprs.size(); i++) {
            if (visitor.transform(primaryKeyExprs.get(i))) {
                b = true;
            }
        }
        for (int i = 0; i < secondaryKeyExprs.size(); i++) {
            if (visitor.transform(secondaryKeyExprs.get(i))) {
                b = true;
            }
        }
        return b;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitTokenizeOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {

            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                target.addAllVariables(sources[0]);
                for (LogicalVariable v : tokenizeVars) {
                    target.addVariable(v);
                }
            }
        };

    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.TOKENIZE;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        IVariableTypeEnvironment env = createPropagatingAllInputsTypeEnvironment(ctx);

        // If the secondary index is not length-partitioned, create one new
        // output variable - token.
        // If the secondary index is length-partitioned, create two new output
        // variables - token, number of token.
        // The type of this variable will be the same as the type of the
        // secondary key. If the secondary is list type, then the element type
        // of the list.
        // Along with PK, the tokenizer will generate [token, number of tokens,
        // PK] pairs.

        for (int i = 0; i < tokenizeVars.size(); i++) {
            env.setVarType(tokenizeVars.get(i), tokenizeVarTypes.get(i));
        }

        return env;
    }

    public List<Mutable<ILogicalExpression>> getPrimaryKeyExpressions() {
        return primaryKeyExprs;
    }

    public IDataSourceIndex<?, ?> getDataSourceIndex() {
        return dataSourceIndex;
    }

    public List<Mutable<ILogicalExpression>> getSecondaryKeyExpressions() {
        return secondaryKeyExprs;
    }

    public List<LogicalVariable> getTokenizeVars() {
        return tokenizeVars;
    }

    public Mutable<ILogicalExpression> getFilterExpression() {
        return filterExpr;
    }

    public Kind getOperation() {
        return operation;
    }

    public boolean isBulkload() {
        return bulkload;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }

    public List<Object> getTokenizeVarTypes() {
        return tokenizeVarTypes;
    }

    public void setAdditionalFilteringExpressions(List<Mutable<ILogicalExpression>> additionalFilteringExpressions) {
        this.additionalFilteringExpressions = additionalFilteringExpressions;
    }

    public List<Mutable<ILogicalExpression>> getAdditionalFilteringExpressions() {
        return additionalFilteringExpressions;
    }

}
