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

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;

public abstract class AbstractUnnestOperator extends AbstractScanOperator {

    protected final Mutable<ILogicalExpression> expression;

    public AbstractUnnestOperator(List<LogicalVariable> variables, Mutable<ILogicalExpression> expression) {
        super(variables);
        this.expression = expression;
    }

    public Mutable<ILogicalExpression> getExpressionRef() {
        return expression;
    }

    @Override
    public boolean isMap() {
        return true;
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        return visitor.transform(expression);
    }

}