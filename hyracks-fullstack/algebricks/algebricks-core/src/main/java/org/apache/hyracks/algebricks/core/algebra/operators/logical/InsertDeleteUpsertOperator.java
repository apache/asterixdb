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
import org.apache.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.PropagatingTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class InsertDeleteUpsertOperator extends AbstractLogicalOperator {

    public enum Kind {
        INSERT,
        DELETE,
        UPSERT
    }

    private final IDataSource<?> dataSource;
    private final Mutable<ILogicalExpression> payloadExpr;
    private final List<Mutable<ILogicalExpression>> primaryKeyExprs;
    private final Kind operation;
    private final boolean bulkload;
    private List<Mutable<ILogicalExpression>> additionalFilteringExpressions;
    private final List<Mutable<ILogicalExpression>> additionalNonFilteringExpressions;
    // previous record (for UPSERT)
    private LogicalVariable prevRecordVar;
    private Object prevRecordType;
    // previous filter (for UPSERT)
    private LogicalVariable prevFilterVar;
    private Object prevFilterType;
    // previous additional fields (for UPSERT)
    private List<LogicalVariable> prevAdditionalNonFilteringVars;
    private List<Object> prevAdditionalNonFilteringTypes;
    // a boolean variable that indicates whether it's a delete operation (false) or upsert operation (true)
    private LogicalVariable upsertIndicatorVar;
    private Object upsertIndicatorVarType;

    public InsertDeleteUpsertOperator(IDataSource<?> dataSource, Mutable<ILogicalExpression> payloadExpr,
            List<Mutable<ILogicalExpression>> primaryKeyExprs,
            List<Mutable<ILogicalExpression>> additionalNonFilteringExpressions, Kind operation, boolean bulkload) {
        this.dataSource = dataSource;
        this.payloadExpr = payloadExpr;
        this.primaryKeyExprs = primaryKeyExprs;
        this.operation = operation;
        this.bulkload = bulkload;
        this.additionalNonFilteringExpressions = additionalNonFilteringExpressions;
    }

    public InsertDeleteUpsertOperator(IDataSource<?> dataSource, Mutable<ILogicalExpression> payloadExpr,
            List<Mutable<ILogicalExpression>> primaryKeyExprs, Kind operation, boolean bulkload) {
        this.dataSource = dataSource;
        this.payloadExpr = payloadExpr;
        this.primaryKeyExprs = primaryKeyExprs;
        this.operation = operation;
        this.bulkload = bulkload;
        this.additionalNonFilteringExpressions = null;
    }

    @Override
    public void recomputeSchema() throws AlgebricksException {
        schema = new ArrayList<LogicalVariable>();
        if (operation == Kind.UPSERT) {
            schema.add(upsertIndicatorVar);
            // The upsert case also produces the previous record
            schema.add(prevRecordVar);
            if (additionalNonFilteringExpressions != null) {
                schema.addAll(prevAdditionalNonFilteringVars);
            }
            if (prevFilterVar != null) {
                schema.add(prevFilterVar);
            }
        }
        schema.addAll(inputs.get(0).getValue().getSchema());
    }

    public void getProducedVariables(Collection<LogicalVariable> producedVariables) {
        if (upsertIndicatorVar != null) {
            producedVariables.add(upsertIndicatorVar);
        }
        if (prevRecordVar != null) {
            producedVariables.add(prevRecordVar);
        }
        if (prevAdditionalNonFilteringVars != null) {
            producedVariables.addAll(prevAdditionalNonFilteringVars);
        }
        if (prevFilterVar != null) {
            producedVariables.add(prevFilterVar);
        }
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform)
            throws AlgebricksException {
        boolean changed = false;
        changed = transform.transform(payloadExpr);
        for (Mutable<ILogicalExpression> e : primaryKeyExprs) {
            changed |= transform.transform(e);
        }
        if (additionalFilteringExpressions != null) {
            for (Mutable<ILogicalExpression> e : additionalFilteringExpressions) {
                changed |= transform.transform(e);
            }
        }
        if (additionalNonFilteringExpressions != null) {
            for (Mutable<ILogicalExpression> e : additionalNonFilteringExpressions) {
                changed |= transform.transform(e);
            }
        }
        return changed;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitInsertDeleteUpsertOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        return true;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {
            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                if (operation == Kind.UPSERT) {
                    target.addVariable(upsertIndicatorVar);
                    target.addVariable(prevRecordVar);
                    if (prevAdditionalNonFilteringVars != null) {
                        for (LogicalVariable var : prevAdditionalNonFilteringVars) {
                            target.addVariable(var);
                        }
                    }
                    if (prevFilterVar != null) {
                        target.addVariable(prevFilterVar);
                    }
                }
                target.addAllVariables(sources[0]);
            }
        };
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.INSERT_DELETE_UPSERT;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        PropagatingTypeEnvironment env = createPropagatingAllInputsTypeEnvironment(ctx);
        if (operation == Kind.UPSERT) {
            env.setVarType(upsertIndicatorVar, upsertIndicatorVarType);
            env.setVarType(prevRecordVar, prevRecordType);
            if (prevAdditionalNonFilteringVars != null) {
                for (int i = 0; i < prevAdditionalNonFilteringVars.size(); i++) {
                    env.setVarType(prevAdditionalNonFilteringVars.get(i), prevAdditionalNonFilteringTypes.get(i));
                }
            }
            if (prevFilterVar != null) {
                env.setVarType(prevFilterVar, prevFilterType);
            }
        }
        return env;
    }

    public List<Mutable<ILogicalExpression>> getPrimaryKeyExpressions() {
        return primaryKeyExprs;
    }

    public IDataSource<?> getDataSource() {
        return dataSource;
    }

    public Mutable<ILogicalExpression> getPayloadExpression() {
        return payloadExpr;
    }

    public Kind getOperation() {
        return operation;
    }

    public boolean isBulkload() {
        return bulkload;
    }

    public List<Mutable<ILogicalExpression>> getAdditionalNonFilteringExpressions() {
        return additionalNonFilteringExpressions;
    }

    public void setAdditionalFilteringExpressions(List<Mutable<ILogicalExpression>> additionalFilteringExpressions) {
        this.additionalFilteringExpressions = additionalFilteringExpressions;
    }

    public List<Mutable<ILogicalExpression>> getAdditionalFilteringExpressions() {
        return additionalFilteringExpressions;
    }

    public LogicalVariable getBeforeOpRecordVar() {
        return prevRecordVar;
    }

    public void setPrevRecordVar(LogicalVariable prevRecordVar) {
        this.prevRecordVar = prevRecordVar;
    }

    public LogicalVariable getUpsertIndicatorVar() {
        return upsertIndicatorVar;
    }

    public void setUpsertIndicatorVar(LogicalVariable upsertIndicatorVar) {
        this.upsertIndicatorVar = upsertIndicatorVar;
    }

    public Object getUpsertIndicatorVarType() {
        return upsertIndicatorVarType;
    }

    public void setUpsertIndicatorVarType(Object upsertIndicatorVarType) {
        this.upsertIndicatorVarType = upsertIndicatorVarType;
    }

    public void setPrevRecordType(Object recordType) {
        prevRecordType = recordType;
    }

    public LogicalVariable getBeforeOpFilterVar() {
        return prevFilterVar;
    }

    public void setPrevFilterVar(LogicalVariable prevFilterVar) {
        this.prevFilterVar = prevFilterVar;
    }

    public Object getPrevFilterType() {
        return prevFilterType;
    }

    public void setPrevFilterType(Object prevFilterType) {
        this.prevFilterType = prevFilterType;
    }

    public List<LogicalVariable> getBeforeOpAdditionalNonFilteringVars() {
        return prevAdditionalNonFilteringVars;
    }

    public void setPrevAdditionalNonFilteringVars(List<LogicalVariable> prevAdditionalNonFilteringVars) {
        this.prevAdditionalNonFilteringVars = prevAdditionalNonFilteringVars;
    }

    public List<Object> getPrevAdditionalNonFilteringTypes() {
        return prevAdditionalNonFilteringTypes;
    }

    public void setPrevAdditionalNonFilteringTypes(List<Object> prevAdditionalNonFilteringTypes) {
        this.prevAdditionalNonFilteringTypes = prevAdditionalNonFilteringTypes;
    }
}
