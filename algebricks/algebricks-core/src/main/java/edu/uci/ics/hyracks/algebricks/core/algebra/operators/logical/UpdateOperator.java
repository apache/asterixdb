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

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypingContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class UpdateOperator extends AbstractLogicalOperator {

    @Override
    public void recomputeSchema() throws AlgebricksException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform transform) throws AlgebricksException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isMap() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        // TODO Auto-generated method stub
        return null;
    }

}
