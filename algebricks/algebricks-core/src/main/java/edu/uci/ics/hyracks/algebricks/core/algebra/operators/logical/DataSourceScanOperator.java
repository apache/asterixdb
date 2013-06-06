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
import java.util.Collection;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypingContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class DataSourceScanOperator extends AbstractScanOperator {
    private IDataSource<?> dataSource;

    private List<LogicalVariable> projectVars;

    private boolean projectPushed = false;

    public DataSourceScanOperator(List<LogicalVariable> variables, IDataSource<?> dataSource) {
        super(variables);
        this.dataSource = dataSource;
        projectVars = new ArrayList<LogicalVariable>();
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.DATASOURCESCAN;
    }

    public IDataSource<?> getDataSource() {
        return dataSource;
    }

    @Override
    public <R, S> R accept(ILogicalOperatorVisitor<R, S> visitor, S arg) throws AlgebricksException {
        return visitor.visitDataScanOperator(this, arg);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean isMap() {
        return false;
    }

    public void addProjectVariables(Collection<LogicalVariable> vars) {
        projectVars.addAll(vars);
        projectPushed = true;
    }

    public List<LogicalVariable> getProjectVariables() {
        return projectVars;
    }

    public boolean isProjectPushed() {
        return projectPushed;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {
            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                if (sources.length > 0) {
                    target.addAllVariables(sources[0]);
                }
                List<LogicalVariable> outputVariables = projectPushed ? projectVars : variables;
                for (LogicalVariable v : outputVariables) {
                    target.addVariable(v);
                }
            }
        };
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        IVariableTypeEnvironment env = createPropagatingAllInputsTypeEnvironment(ctx);
        Object[] types = dataSource.getSchemaTypes();
        int i = 0;
        for (LogicalVariable v : variables) {
            env.setVarType(v, types[i]);
            ++i;
        }
        return env;
    }
}