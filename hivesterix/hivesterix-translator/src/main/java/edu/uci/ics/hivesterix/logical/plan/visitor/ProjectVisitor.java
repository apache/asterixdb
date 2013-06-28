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
package edu.uci.ics.hivesterix.logical.plan.visitor;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;

import edu.uci.ics.hivesterix.logical.plan.visitor.base.DefaultVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.base.Translator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;

public class ProjectVisitor extends DefaultVisitor {

    /**
     * translate project operator
     */
    @Override
    public Mutable<ILogicalOperator> visit(SelectOperator operator, Mutable<ILogicalOperator> AlgebricksParentOperator,
            Translator t) {

        SelectDesc desc = (SelectDesc) operator.getConf();

        if (desc == null)
            return null;

        List<ExprNodeDesc> cols = desc.getColList();

        if (cols == null)
            return null;

        // insert assign operator if necessary
        ArrayList<LogicalVariable> variables = new ArrayList<LogicalVariable>();

        for (ExprNodeDesc expr : cols)
            t.rewriteExpression(expr);

        ILogicalOperator assignOp = t.getAssignOperator(AlgebricksParentOperator, cols, variables);
        ILogicalOperator currentOperator = null;
        if (assignOp != null) {
            currentOperator = assignOp;
            AlgebricksParentOperator = new MutableObject<ILogicalOperator>(currentOperator);
        }

        currentOperator = new ProjectOperator(variables);
        currentOperator.getInputs().add(AlgebricksParentOperator);
        t.rewriteOperatorOutputSchema(variables, operator);
        return new MutableObject<ILogicalOperator>(currentOperator);
    }

}
