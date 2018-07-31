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

public class IndexInsertDeleteUpsertOperator extends AbstractLogicalOperator {

    private final IDataSourceIndex<?, ?> dataSourceIndex;
    private final List<Mutable<ILogicalExpression>> primaryKeyExprs;
    // In the bulk-load case on ngram or keyword index,
    // it contains [token, number of token] or [token].
    // Otherwise, it contains secondary key information.
    private final List<Mutable<ILogicalExpression>> secondaryKeyExprs;
    private final Mutable<ILogicalExpression> filterExpr;
    private final Kind operation;
    private final boolean bulkload;
    private List<Mutable<ILogicalExpression>> additionalFilteringExpressions;
    // used for upsert operations
    private List<Mutable<ILogicalExpression>> prevSecondaryKeyExprs;
    private Mutable<ILogicalExpression> prevAdditionalFilteringExpression;
    private Mutable<ILogicalExpression> upsertIndicatorExpr;
    private final int numberOfAdditionalNonFilteringFields;

    public IndexInsertDeleteUpsertOperator(IDataSourceIndex<?, ?> dataSourceIndex,
            List<Mutable<ILogicalExpression>> primaryKeyExprs, List<Mutable<ILogicalExpression>> secondaryKeyExprs,
            Mutable<ILogicalExpression> filterExpr, Kind operation, boolean bulkload,
            int numberOfAdditionalNonFilteringFields) {
        this.dataSourceIndex = dataSourceIndex;
        this.primaryKeyExprs = primaryKeyExprs;
        this.secondaryKeyExprs = secondaryKeyExprs;
        this.filterExpr = filterExpr;
        this.operation = operation;
        this.bulkload = bulkload;
        this.numberOfAdditionalNonFilteringFields = numberOfAdditionalNonFilteringFields;
    }

    @Override
    public void recomputeSchema() throws AlgebricksException {
        schema = new ArrayList<LogicalVariable>();
        schema.addAll(inputs.get(0).getValue().getSchema());
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
        // Additional Filtering <For upsert>
        if (additionalFilteringExpressions != null) {
            for (int i = 0; i < additionalFilteringExpressions.size(); i++) {
                if (visitor.transform(additionalFilteringExpressions.get(i))) {
                    b = true;
                }
            }
        }

        // Upsert indicator var <For upsert>
        if (upsertIndicatorExpr != null && visitor.transform(upsertIndicatorExpr)) {
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
        // Old Filtering <For upsert>
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
    public boolean isMap() {
        return false;
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

    public Mutable<ILogicalExpression> getFilterExpression() {
        return filterExpr;
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

    public Mutable<ILogicalExpression> getUpsertIndicatorExpr() {
        return upsertIndicatorExpr;
    }

    public void setUpsertIndicatorExpr(Mutable<ILogicalExpression> upsertIndicatorExpr) {
        this.upsertIndicatorExpr = upsertIndicatorExpr;
    }
}
