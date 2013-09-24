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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import edu.uci.ics.hivesterix.logical.plan.visitor.base.DefaultVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.base.Translator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;

@SuppressWarnings("rawtypes")
public class MapJoinVisitor extends DefaultVisitor {

    /**
     * map a join operator (in hive) to its parent operators (in asterix)
     */
    private HashMap<Operator, List<Mutable<ILogicalOperator>>> opMap = new HashMap<Operator, List<Mutable<ILogicalOperator>>>();

    @Override
    public Mutable<ILogicalOperator> visit(MapJoinOperator operator,
            Mutable<ILogicalOperator> AlgebricksParentOperatorRef, Translator t) {
        List<Operator<? extends OperatorDesc>> joinSrc = operator.getParentOperators();
        List<Mutable<ILogicalOperator>> parents = opMap.get(operator);
        if (parents == null) {
            parents = new ArrayList<Mutable<ILogicalOperator>>();
            opMap.put(operator, parents);
        }
        parents.add(AlgebricksParentOperatorRef);
        if (joinSrc.size() != parents.size())
            return null;

        ILogicalOperator currentOperator;
        // make an map join operator
        // TODO: will have trouble for n-way joins
        MapJoinDesc joinDesc = (MapJoinDesc) operator.getConf();

        Map<Byte, List<ExprNodeDesc>> keyMap = joinDesc.getKeys();
        // get the projection expression (already re-written) from each source
        // table
        Map<Byte, List<ExprNodeDesc>> exprMap = joinDesc.getExprs();

        int inputSize = operator.getParentOperators().size();
        // get a list of reduce sink descs (input descs)

        // get the parent operator
        List<Mutable<ILogicalOperator>> parentOps = parents;

        List<String> fieldNames = new ArrayList<String>();
        List<TypeInfo> types = new ArrayList<TypeInfo>();
        for (Operator ts : joinSrc) {
            List<ColumnInfo> columns = ts.getSchema().getSignature();
            for (ColumnInfo col : columns) {
                fieldNames.add(col.getInternalName());
                types.add(col.getType());
            }
        }

        // get number of equality conjunctions in the final join condition
        Set<Entry<Byte, List<ExprNodeDesc>>> keyEntries = keyMap.entrySet();
        Iterator<Entry<Byte, List<ExprNodeDesc>>> entry = keyEntries.iterator();

        int size = 0;
        if (entry.hasNext())
            size = entry.next().getValue().size();

        // make up the join conditon expression
        List<ExprNodeDesc> joinConditionChildren = new ArrayList<ExprNodeDesc>();
        for (int i = 0; i < size; i++) {
            // create a join key pair
            List<ExprNodeDesc> keyPair = new ArrayList<ExprNodeDesc>();
            for (int j = 0; j < inputSize; j++) {
                keyPair.add(keyMap.get(Byte.valueOf((byte) j)).get(i));
            }
            // create a hive equal condition
            ExprNodeDesc equality = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
                    new GenericUDFOPEqual(), keyPair);
            // add the equal condition to the conjunction list
            joinConditionChildren.add(equality);
        }
        // get final conjunction expression
        ExprNodeDesc conjunct = null;

        if (joinConditionChildren.size() > 1)
            conjunct = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, new GenericUDFOPAnd(),
                    joinConditionChildren);
        else if (joinConditionChildren.size() == 1)
            conjunct = joinConditionChildren.get(0);
        else {
            // there is no join equality condition, full outer join
            conjunct = new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, new Boolean(true));
        }
        // get an ILogicalExpression from hive's expression
        Mutable<ILogicalExpression> expression = t.translateScalarFucntion(conjunct);

        ArrayList<LogicalVariable> left = new ArrayList<LogicalVariable>();
        ArrayList<LogicalVariable> right = new ArrayList<LogicalVariable>();

        Set<Entry<Byte, List<ExprNodeDesc>>> kentries = keyMap.entrySet();
        Iterator<Entry<Byte, List<ExprNodeDesc>>> kiterator = kentries.iterator();
        int iteration = 0;
        ILogicalOperator assignOperator = null;
        while (kiterator.hasNext()) {
            List<ExprNodeDesc> outputExprs = kiterator.next().getValue();

            if (iteration == 0)
                assignOperator = t.getAssignOperator(AlgebricksParentOperatorRef, outputExprs, left);
            else
                assignOperator = t.getAssignOperator(AlgebricksParentOperatorRef, outputExprs, right);

            if (assignOperator != null) {
                currentOperator = assignOperator;
                AlgebricksParentOperatorRef = new MutableObject<ILogicalOperator>(currentOperator);
            }
            iteration++;
        }

        List<Mutable<ILogicalOperator>> inputs = parentOps;

        // get the join operator
        currentOperator = new InnerJoinOperator(expression);

        // set the inputs from asterix join operator
        for (Mutable<ILogicalOperator> input : inputs)
            currentOperator.getInputs().add(input);
        AlgebricksParentOperatorRef = new MutableObject<ILogicalOperator>(currentOperator);

        // add assign and project operator
        // output variables
        ArrayList<LogicalVariable> variables = new ArrayList<LogicalVariable>();
        Set<Entry<Byte, List<ExprNodeDesc>>> entries = exprMap.entrySet();
        Iterator<Entry<Byte, List<ExprNodeDesc>>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            List<ExprNodeDesc> outputExprs = iterator.next().getValue();
            assignOperator = t.getAssignOperator(AlgebricksParentOperatorRef, outputExprs, variables);

            if (assignOperator != null) {
                currentOperator = assignOperator;
                AlgebricksParentOperatorRef = new MutableObject<ILogicalOperator>(currentOperator);
            }
        }

        currentOperator = new ProjectOperator(variables);
        currentOperator.getInputs().add(AlgebricksParentOperatorRef);
        t.rewriteOperatorOutputSchema(variables, operator);
        // opMap.clear();
        return new MutableObject<ILogicalOperator>(currentOperator);
    }
}
