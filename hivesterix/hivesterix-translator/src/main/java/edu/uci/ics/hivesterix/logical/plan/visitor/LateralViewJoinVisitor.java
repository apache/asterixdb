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
import org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.UDTFOperator;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import edu.uci.ics.hivesterix.logical.plan.visitor.base.DefaultVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.base.Translator;
import edu.uci.ics.hivesterix.runtime.jobgen.Schema;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;

/**
 * The lateral view join operator is used for FROM src LATERAL VIEW udtf()...
 * This operator was implemented with the following operator DAG in mind.
 * For a query such as
 * SELECT pageid, adid.* FROM example_table LATERAL VIEW explode(adid_list) AS
 * adid
 * The top of the operator DAG will look similar to
 * [Table Scan] | [Lateral View Forward] / \ [Select](*) [Select](adid_list) | |
 * | [UDTF] (explode) \ / [Lateral View Join] | | [Select] (pageid, adid.*) |
 * ....
 * Rows from the table scan operator are first to a lateral view forward
 * operator that just forwards the row and marks the start of a LV. The select
 * operator on the left picks all the columns while the select operator on the
 * right picks only the columns needed by the UDTF.
 * The output of select in the left branch and output of the UDTF in the right
 * branch are then sent to the lateral view join (LVJ). In most cases, the UDTF
 * will generate > 1 row for every row received from the TS, while the left
 * select operator will generate only one. For each row output from the TS, the
 * LVJ outputs all possible rows that can be created by joining the row from the
 * left select and one of the rows output from the UDTF.
 * Additional lateral views can be supported by adding a similar DAG after the
 * previous LVJ operator.
 */

@SuppressWarnings("rawtypes")
public class LateralViewJoinVisitor extends DefaultVisitor {

    private UDTFDesc udtf;

    private List<Mutable<ILogicalOperator>> parents = new ArrayList<Mutable<ILogicalOperator>>();

    @Override
    public Mutable<ILogicalOperator> visit(LateralViewJoinOperator operator,
            Mutable<ILogicalOperator> AlgebricksParentOperatorRef, Translator t) throws AlgebricksException {

        parents.add(AlgebricksParentOperatorRef);
        if (operator.getParentOperators().size() > parents.size()) {
            return null;
        }

        Operator parent0 = operator.getParentOperators().get(0);
        ILogicalOperator parentOperator;
        ILogicalExpression unnestArg;
        if (parent0 instanceof UDTFOperator) {
            List<LogicalVariable> unnestVars = new ArrayList<LogicalVariable>();
            VariableUtilities.getLiveVariables(parents.get(1).getValue(), unnestVars);
            unnestArg = new VariableReferenceExpression(unnestVars.get(0));
            parentOperator = parents.get(1).getValue();
        } else {
            List<LogicalVariable> unnestVars = new ArrayList<LogicalVariable>();
            VariableUtilities.getLiveVariables(parents.get(0).getValue(), unnestVars);
            unnestArg = new VariableReferenceExpression(unnestVars.get(0));
            parentOperator = parents.get(0).getValue();
        }

        LogicalVariable var = t.getVariable(udtf.toString(), TypeInfoFactory.unknownTypeInfo);

        Mutable<ILogicalExpression> unnestExpr = t.translateUnnestFunction(udtf, new MutableObject<ILogicalExpression>(
                unnestArg));
        ILogicalOperator currentOperator = new UnnestOperator(var, unnestExpr);

        List<LogicalVariable> outputVars = new ArrayList<LogicalVariable>();
        VariableUtilities.getLiveVariables(parentOperator, outputVars);
        outputVars.add(var);
        currentOperator.getInputs().add(new MutableObject<ILogicalOperator>(parentOperator));

        parents.clear();
        udtf = null;
        t.rewriteOperatorOutputSchema(outputVars, operator);
        return new MutableObject<ILogicalOperator>(currentOperator);
    }

    @Override
    public Mutable<ILogicalOperator> visit(UDTFOperator operator,
            Mutable<ILogicalOperator> AlgebricksParentOperatorRef, Translator t) {
        Schema currentSchema = t.generateInputSchema(operator.getParentOperators().get(0));
        udtf = (UDTFDesc) operator.getConf();

        // populate the schema from upstream operator
        operator.setSchema(operator.getParentOperators().get(0).getSchema());
        List<LogicalVariable> latestOutputSchema = t.getVariablesFromSchema(currentSchema);
        t.rewriteOperatorOutputSchema(latestOutputSchema, operator);
        return null;
    }

}
