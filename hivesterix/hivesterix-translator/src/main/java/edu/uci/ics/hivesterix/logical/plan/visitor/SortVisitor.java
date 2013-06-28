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
import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;

import edu.uci.ics.hivesterix.logical.plan.visitor.base.DefaultVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.base.Translator;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.OrderColumn;

public class SortVisitor extends DefaultVisitor {

    @SuppressWarnings("rawtypes")
    @Override
    public Mutable<ILogicalOperator> visit(ReduceSinkOperator operator,
            Mutable<ILogicalOperator> AlgebricksParentOperatorRef, Translator t) throws AlgebricksException {
        ReduceSinkDesc desc = (ReduceSinkDesc) operator.getConf();
        Operator downStream = (Operator) operator.getChildOperators().get(0);
        List<ExprNodeDesc> keys = desc.getKeyCols();
        if (!(downStream instanceof ExtractOperator && desc.getNumReducers() == 1 && keys.size() > 0)) {
            return null;
        }

        List<ExprNodeDesc> schema = new ArrayList<ExprNodeDesc>();
        List<ExprNodeDesc> values = desc.getValueCols();
        List<ExprNodeDesc> partitionCols = desc.getPartitionCols();
        for (ExprNodeDesc key : keys) {
            t.rewriteExpression(key);
        }
        for (ExprNodeDesc value : values) {
            t.rewriteExpression(value);
        }
        for (ExprNodeDesc col : partitionCols) {
            t.rewriteExpression(col);
        }

        // add a order-by operator and limit if any
        List<Pair<IOrder, Mutable<ILogicalExpression>>> pairs = new ArrayList<Pair<IOrder, Mutable<ILogicalExpression>>>();
        char[] orders = desc.getOrder().toCharArray();
        int i = 0;
        for (ExprNodeDesc key : keys) {
            Mutable<ILogicalExpression> expr = t.translateScalarFucntion(key);
            IOrder order = orders[i] == '+' ? OrderOperator.ASC_ORDER : OrderOperator.DESC_ORDER;

            Pair<IOrder, Mutable<ILogicalExpression>> pair = new Pair<IOrder, Mutable<ILogicalExpression>>(order, expr);
            pairs.add(pair);
            i++;
        }

        // get input variables
        ArrayList<LogicalVariable> inputVariables = new ArrayList<LogicalVariable>();
        VariableUtilities.getProducedVariables(AlgebricksParentOperatorRef.getValue(), inputVariables);

        ArrayList<LogicalVariable> keyVariables = new ArrayList<LogicalVariable>();
        ILogicalOperator currentOperator;
        ILogicalOperator assignOp = t.getAssignOperator(AlgebricksParentOperatorRef, keys, keyVariables);
        if (assignOp != null) {
            currentOperator = assignOp;
            AlgebricksParentOperatorRef = new MutableObject<ILogicalOperator>(currentOperator);
        }

        OrderColumn[] keyColumns = new OrderColumn[keyVariables.size()];

        for (int j = 0; j < keyColumns.length; j++)
            keyColumns[j] = new OrderColumn(keyVariables.get(j), pairs.get(j).first.getKind());

        // handle order operator
        currentOperator = new OrderOperator(pairs);
        currentOperator.getInputs().add(AlgebricksParentOperatorRef);
        AlgebricksParentOperatorRef = new MutableObject<ILogicalOperator>(currentOperator);

        // project back, remove generated sort-key columns if any
        if (assignOp != null) {
            currentOperator = new ProjectOperator(inputVariables);
            currentOperator.getInputs().add(AlgebricksParentOperatorRef);
            AlgebricksParentOperatorRef = new MutableObject<ILogicalOperator>(currentOperator);
        }

        /**
         * a special rule for hive's order by output schema of reduce sink
         * operator only contains the columns
         */
        for (ExprNodeDesc value : values) {
            schema.add(value);
        }

        ArrayList<LogicalVariable> variables = new ArrayList<LogicalVariable>();
        ILogicalOperator assignOperator = t.getAssignOperator(AlgebricksParentOperatorRef, schema, variables);
        t.rewriteOperatorOutputSchema(variables, operator);

        if (assignOperator != null) {
            currentOperator = assignOperator;
            AlgebricksParentOperatorRef = new MutableObject<ILogicalOperator>(currentOperator);
        }
        return new MutableObject<ILogicalOperator>(currentOperator);
    }
}
