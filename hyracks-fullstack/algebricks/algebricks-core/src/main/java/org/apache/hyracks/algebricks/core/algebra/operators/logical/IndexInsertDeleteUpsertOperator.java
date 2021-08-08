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
import java.util.Collection;
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

/**
 * Logical operator for handling secondary index maintenance / loading.
 * <p>
 *
 * In both cases (whether the index is on an atomic field or an array field):
 * <p>
 * Primary keys will be given in {@link #primaryKeyExprs}. {@link #operation} specifies the type of index maintenance to
 * perform. In the case of bulk-loading, {@link #operation} will be INSERT and the {@link #bulkload} flag will be
 * raised. {@link #additionalFilteringExpressions} and {@link #numberOfAdditionalNonFilteringFields} refers to the
 * additionalFilteringExpressions, numberOfAdditionalNonFilteringFields found in the corresponding primary index
 * {@link InsertDeleteUpsertOperator} (i.e. to specify LSM filters). {@link #operationExpr} also originates from
 * {@link InsertDeleteUpsertOperator}, and is only set when the operation is of kind UPSERT.
 * <p>
 *
 * If the SIDX is on an atomic field <b>or</b> on an array field w/ a bulk-load operation:
 * <p>
 * We specify secondary key information in {@link #secondaryKeyExprs}. If we may encounter nullable keys, then we
 * specify a {@link #filterExpr} to be evaluated inside the runtime. If the operation is of kind UPSERT, then we must
 * also specify previous secondary key information in {@link #prevSecondaryKeyExprs}. If
 * {@link #additionalFilteringExpressions} has been set, then {@link #prevAdditionalFilteringExpression} should also be
 * set.
 *
 * <p>
 * If the SIDX is on an array field <b>and</b> we are not performing a bulk-load operation:
 * <p>
 * We <b>do not</b> specify secondary key information in {@link #secondaryKeyExprs} (this is null). Instead, we specify
 * how to extract secondary keys using {@link #nestedPlans}. If we may encounter nullable keys, then we <b>do not</b>
 * specify a {@link #filterExpr} (this is null). Instead, this filter must be attached to the top of the nested plan
 * itself. If the operation is not of type UPSERT, then we must only have one nested plan. Otherwise, the second nested
 * plan must specify how to extract secondary keys from the previous record. {@link #prevSecondaryKeyExprs} and
 * {@link #prevAdditionalFilteringExpression} will always be null here, even if the operation is UPSERT.
 *
 */
public class IndexInsertDeleteUpsertOperator extends AbstractOperatorWithNestedPlans {

    private final IDataSourceIndex<?, ?> dataSourceIndex;
    private final List<Mutable<ILogicalExpression>> primaryKeyExprs;
    // In the bulk-load case on ngram or keyword index,
    // it contains [token, number of token] or [token].
    // In the non bulk-load array-index case, it contains nothing.
    // Otherwise, it contains secondary key information.
    private List<Mutable<ILogicalExpression>> secondaryKeyExprs;
    private Mutable<ILogicalExpression> filterExpr;
    private Mutable<ILogicalExpression> beforeOpFilterExpr;
    private final Kind operation;
    private final boolean bulkload;
    private List<Mutable<ILogicalExpression>> additionalFilteringExpressions;
    // used for upsert operations
    private List<Mutable<ILogicalExpression>> prevSecondaryKeyExprs;
    private Mutable<ILogicalExpression> prevAdditionalFilteringExpression;
    private Mutable<ILogicalExpression> operationExpr;
    private final int numberOfAdditionalNonFilteringFields;

    public IndexInsertDeleteUpsertOperator(IDataSourceIndex<?, ?> dataSourceIndex,
            List<Mutable<ILogicalExpression>> primaryKeyExprs, List<Mutable<ILogicalExpression>> secondaryKeyExprs,
            Mutable<ILogicalExpression> filterExpr, Mutable<ILogicalExpression> beforeOpFilterExpr, Kind operation,
            boolean bulkload, int numberOfAdditionalNonFilteringFields) {
        this.dataSourceIndex = dataSourceIndex;
        this.primaryKeyExprs = primaryKeyExprs;
        this.secondaryKeyExprs = secondaryKeyExprs;
        this.filterExpr = filterExpr;
        this.beforeOpFilterExpr = beforeOpFilterExpr;
        this.operation = operation;
        this.bulkload = bulkload;
        this.numberOfAdditionalNonFilteringFields = numberOfAdditionalNonFilteringFields;
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        boolean b = false;
        // Primary
        for (int i = 0; i < primaryKeyExprs.size(); i++) {
            if (visitor.transform(primaryKeyExprs.get(i))) {
                b = true;
            }
        }
        // Secondary
        for (int i = 0; i < secondaryKeyExprs.size(); i++) {
            if (visitor.transform(secondaryKeyExprs.get(i))) {
                b = true;
            }
        }
        // Filtering
        if (filterExpr != null) {
            if (visitor.transform(filterExpr)) {
                b = true;
            }
        }
        // Old Filtering <For upsert>
        if (beforeOpFilterExpr != null) {
            if (visitor.transform(beforeOpFilterExpr)) {
                b = true;
            }
        }
        // Additional Filtering <For upsert>
        if (additionalFilteringExpressions != null) {
            for (int i = 0; i < additionalFilteringExpressions.size(); i++) {
                if (visitor.transform(additionalFilteringExpressions.get(i))) {
                    b = true;
                }
            }
        }
        // Operation indicator var <For upsert>
        if (operationExpr != null && visitor.transform(operationExpr)) {
            b = true;
        }
        // Old secondary <For upsert>
        if (prevSecondaryKeyExprs != null) {
            for (int i = 0; i < prevSecondaryKeyExprs.size(); i++) {
                if (visitor.transform(prevSecondaryKeyExprs.get(i))) {
                    b = true;
                }
            }
        }
        // Old Additional Filtering <For upsert>
        if (prevAdditionalFilteringExpression != null) {
            visitor.transform(prevAdditionalFilteringExpression);
        }
        return b;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitIndexInsertDeleteUpsertOperator(this, arg);
    }

    @Override
    public void getUsedVariablesExceptNestedPlans(Collection<LogicalVariable> vars) {
        for (Mutable<ILogicalExpression> e : getPrimaryKeyExpressions()) {
            e.getValue().getUsedVariables(vars);
        }
        for (Mutable<ILogicalExpression> e : getSecondaryKeyExpressions()) {
            e.getValue().getUsedVariables(vars);
        }
        if (getFilterExpression() != null) {
            getFilterExpression().getValue().getUsedVariables(vars);
        }
        if (getBeforeOpFilterExpression() != null) {
            getBeforeOpFilterExpression().getValue().getUsedVariables(vars);
        }
        if (getAdditionalFilteringExpressions() != null) {
            for (Mutable<ILogicalExpression> e : getAdditionalFilteringExpressions()) {
                e.getValue().getUsedVariables(vars);
            }
        }
        if (getPrevAdditionalFilteringExpression() != null) {
            getPrevAdditionalFilteringExpression().getValue().getUsedVariables(vars);
        }
        if (getPrevSecondaryKeyExprs() != null) {
            for (Mutable<ILogicalExpression> e : getPrevSecondaryKeyExprs()) {
                e.getValue().getUsedVariables(vars);
            }
        }
        if (getOperationExpr() != null) {
            getOperationExpr().getValue().getUsedVariables(vars);
        }
    }

    @Override
    public void getProducedVariablesExceptNestedPlans(Collection<LogicalVariable> vars) {
        // Do nothing (no variables are produced here).
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ALL;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.INDEX_INSERT_DELETE_UPSERT;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        return createPropagatingAllInputsTypeEnvironment(ctx);
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<>();
        schema.addAll(inputs.get(0).getValue().getSchema());
    }

    public List<Mutable<ILogicalExpression>> getPrimaryKeyExpressions() {
        return primaryKeyExprs;
    }

    public IDataSourceIndex<?, ?> getDataSourceIndex() {
        return dataSourceIndex;
    }

    public String getIndexName() {
        return dataSourceIndex.getId().toString();
    }

    public List<Mutable<ILogicalExpression>> getSecondaryKeyExpressions() {
        return secondaryKeyExprs;
    }

    public void setSecondaryKeyExprs(List<Mutable<ILogicalExpression>> secondaryKeyExprs) {
        this.secondaryKeyExprs = secondaryKeyExprs;
    }

    public Mutable<ILogicalExpression> getFilterExpression() {
        return filterExpr;
    }

    public Mutable<ILogicalExpression> getBeforeOpFilterExpression() {
        return beforeOpFilterExpr;
    }

    public void setFilterExpression(Mutable<ILogicalExpression> filterExpr) {
        this.filterExpr = filterExpr;
    }

    public void setBeforeOpFilterExpression(Mutable<ILogicalExpression> beforeOpFilterExpr) {
        this.beforeOpFilterExpr = beforeOpFilterExpr;
    }

    public Kind getOperation() {
        return operation;
    }

    public boolean isBulkload() {
        return bulkload;
    }

    public void setAdditionalFilteringExpressions(List<Mutable<ILogicalExpression>> additionalFilteringExpressions) {
        this.additionalFilteringExpressions = additionalFilteringExpressions;
    }

    public List<Mutable<ILogicalExpression>> getAdditionalFilteringExpressions() {
        return additionalFilteringExpressions;
    }

    public List<Mutable<ILogicalExpression>> getPrevSecondaryKeyExprs() {
        return prevSecondaryKeyExprs;
    }

    public void setBeforeOpSecondaryKeyExprs(List<Mutable<ILogicalExpression>> prevSecondaryKeyExprs) {
        this.prevSecondaryKeyExprs = prevSecondaryKeyExprs;
    }

    public Mutable<ILogicalExpression> getPrevAdditionalFilteringExpression() {
        return prevAdditionalFilteringExpression;
    }

    public void setBeforeOpAdditionalFilteringExpression(
            Mutable<ILogicalExpression> prevAdditionalFilteringExpression) {
        this.prevAdditionalFilteringExpression = prevAdditionalFilteringExpression;
    }

    public int getNumberOfAdditionalNonFilteringFields() {
        return numberOfAdditionalNonFilteringFields;
    }

    public Mutable<ILogicalExpression> getOperationExpr() {
        return operationExpr;
    }

    public void setOperationExpr(Mutable<ILogicalExpression> operationExpr) {
        this.operationExpr = operationExpr;
    }
}
