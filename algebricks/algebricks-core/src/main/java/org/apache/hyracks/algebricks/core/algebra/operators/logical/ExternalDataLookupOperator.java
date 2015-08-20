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
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypingContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.NonPropagatingTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class ExternalDataLookupOperator extends AbstractDataSourceOperator {

    private final List<Object> variableTypes;
    protected final Mutable<ILogicalExpression> expression;
    private final boolean propagateInput;

    public ExternalDataLookupOperator(List<LogicalVariable> variables, Mutable<ILogicalExpression> expression,
            List<Object> variableTypes, boolean propagateInput, IDataSource<?> dataSource) {
        super(variables, dataSource);
        this.expression = expression;
        this.variableTypes = variableTypes;
        this.propagateInput = propagateInput;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.EXTERNAL_LOOKUP;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        IVariableTypeEnvironment env = null;
        if (propagateInput) {
            env = createPropagatingAllInputsTypeEnvironment(ctx);
        } else {
            env = new NonPropagatingTypeEnvironment(ctx.getExpressionTypeComputer(), ctx.getMetadataProvider());
        }
        env.setVarType(variables.get(0), variableTypes.get(0));
        return env;
    }

    public List<Object> getVariableTypes() {
        return variableTypes;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitExternalDataLookupOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        return false;
    }

    public boolean isPropagateInput() {
        return propagateInput;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {
            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                if (propagateInput) {
                    ArrayList<LogicalVariable> usedVariables = new ArrayList<LogicalVariable>();
                    VariableUtilities.getUsedVariables(ExternalDataLookupOperator.this, usedVariables);
                    int numOfSources = sources.length;
                    for (int i = 0; i < numOfSources; i++) {
                        for (LogicalVariable v : sources[i]) {
                            if (!usedVariables.contains(v)) {
                                target.addVariable(v);
                            }
                        }
                    }
                }
                target.addVariable(variables.get(0));
            }
        };
    }

    public Mutable<ILogicalExpression> getExpressionRef() {
        return expression;
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        return visitor.transform(expression);
    }
}