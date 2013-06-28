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
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
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
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;

@SuppressWarnings("rawtypes")
public class JoinVisitor extends DefaultVisitor {

    /**
     * reduce sink operator to variables
     */
    private HashMap<Operator, List<LogicalVariable>> reduceSinkToKeyVariables = new HashMap<Operator, List<LogicalVariable>>();

    /**
     * reduce sink operator to variables
     */
    private HashMap<Operator, List<String>> reduceSinkToFieldNames = new HashMap<Operator, List<String>>();

    /**
     * reduce sink operator to variables
     */
    private HashMap<Operator, List<TypeInfo>> reduceSinkToTypes = new HashMap<Operator, List<TypeInfo>>();

    /**
     * map a join operator (in hive) to its parent operators (in hive)
     */
    private HashMap<Operator, List<Operator>> operatorToHiveParents = new HashMap<Operator, List<Operator>>();

    /**
     * map a join operator (in hive) to its parent operators (in asterix)
     */
    private HashMap<Operator, List<ILogicalOperator>> operatorToAsterixParents = new HashMap<Operator, List<ILogicalOperator>>();

    /**
     * the latest traversed reduce sink operator
     */
    private Operator latestReduceSink = null;

    /**
     * the latest generated parent for join
     */
    private ILogicalOperator latestAlgebricksOperator = null;

    /**
     * process a join operator
     */
    @Override
    public Mutable<ILogicalOperator> visit(JoinOperator operator, Mutable<ILogicalOperator> AlgebricksParentOperator,
            Translator t) {
        latestAlgebricksOperator = AlgebricksParentOperator.getValue();
        translateJoinOperatorPreprocess(operator, t);
        List<Operator> parents = operatorToHiveParents.get(operator);
        if (parents.size() < operator.getParentOperators().size()) {
            return null;
        } else {
            ILogicalOperator joinOp = translateJoinOperator(operator, AlgebricksParentOperator, t);
            // clearStatus();
            return new MutableObject<ILogicalOperator>(joinOp);
        }
    }

    private void reorder(Byte[] order, List<ILogicalOperator> parents, List<Operator> hiveParents) {
        ILogicalOperator[] lops = new ILogicalOperator[parents.size()];
        Operator[] ops = new Operator[hiveParents.size()];

        for (Operator op : hiveParents) {
            ReduceSinkOperator rop = (ReduceSinkOperator) op;
            ReduceSinkDesc rdesc = rop.getConf();
            int tag = rdesc.getTag();

            int index = -1;
            for (int i = 0; i < order.length; i++)
                if (order[i] == tag) {
                    index = i;
                    break;
                }
            lops[index] = parents.get(hiveParents.indexOf(op));
            ops[index] = op;
        }

        parents.clear();
        hiveParents.clear();

        for (int i = 0; i < lops.length; i++) {
            parents.add(lops[i]);
            hiveParents.add(ops[i]);
        }
    }

    /**
     * translate a hive join operator to asterix join operator->assign
     * operator->project operator
     * 
     * @param parentOperator
     * @param operator
     * @return
     */
    private ILogicalOperator translateJoinOperator(Operator operator, Mutable<ILogicalOperator> parentOperator,
            Translator t) {

        JoinDesc joinDesc = (JoinDesc) operator.getConf();

        // get the projection expression (already re-written) from each source
        // table
        Map<Byte, List<ExprNodeDesc>> exprMap = joinDesc.getExprs();
        reorder(joinDesc.getTagOrder(), operatorToAsterixParents.get(operator), operatorToHiveParents.get(operator));

        // make an reduce join operator
        ILogicalOperator currentOperator = generateJoinTree(joinDesc.getCondsList(),
                operatorToAsterixParents.get(operator), operatorToHiveParents.get(operator), 0, t);
        parentOperator = new MutableObject<ILogicalOperator>(currentOperator);

        // add assign and project operator on top of a join
        // output variables
        ArrayList<LogicalVariable> variables = new ArrayList<LogicalVariable>();
        Set<Entry<Byte, List<ExprNodeDesc>>> entries = exprMap.entrySet();
        Iterator<Entry<Byte, List<ExprNodeDesc>>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            List<ExprNodeDesc> outputExprs = iterator.next().getValue();
            ILogicalOperator assignOperator = t.getAssignOperator(parentOperator, outputExprs, variables);

            if (assignOperator != null) {
                currentOperator = assignOperator;
                parentOperator = new MutableObject<ILogicalOperator>(currentOperator);
            }
        }

        ILogicalOperator po = new ProjectOperator(variables);
        po.getInputs().add(parentOperator);
        t.rewriteOperatorOutputSchema(variables, operator);
        return po;
    }

    /**
     * deal with reduce sink operator for the case of join
     */
    @Override
    public Mutable<ILogicalOperator> visit(ReduceSinkOperator operator, Mutable<ILogicalOperator> parentOperator,
            Translator t) {

        Operator downStream = (Operator) operator.getChildOperators().get(0);
        if (!(downStream instanceof JoinOperator))
            return null;

        ReduceSinkDesc desc = (ReduceSinkDesc) operator.getConf();
        List<ExprNodeDesc> keys = desc.getKeyCols();
        List<ExprNodeDesc> values = desc.getValueCols();
        List<ExprNodeDesc> partitionCols = desc.getPartitionCols();

        /**
         * rewrite key, value, paritioncol expressions
         */
        for (ExprNodeDesc key : keys)
            t.rewriteExpression(key);
        for (ExprNodeDesc value : values)
            t.rewriteExpression(value);
        for (ExprNodeDesc col : partitionCols)
            t.rewriteExpression(col);

        ILogicalOperator currentOperator = null;

        // add assign operator for keys if necessary
        ArrayList<LogicalVariable> keyVariables = new ArrayList<LogicalVariable>();
        ILogicalOperator assignOperator = t.getAssignOperator(parentOperator, keys, keyVariables);
        if (assignOperator != null) {
            currentOperator = assignOperator;
            parentOperator = new MutableObject<ILogicalOperator>(currentOperator);
        }

        // add assign operator for values if necessary
        ArrayList<LogicalVariable> variables = new ArrayList<LogicalVariable>();
        assignOperator = t.getAssignOperator(parentOperator, values, variables);
        if (assignOperator != null) {
            currentOperator = assignOperator;
            parentOperator = new MutableObject<ILogicalOperator>(currentOperator);
        }

        // unified schema: key, value
        ArrayList<LogicalVariable> unifiedKeyValues = new ArrayList<LogicalVariable>();
        unifiedKeyValues.addAll(keyVariables);
        for (LogicalVariable value : variables)
            if (keyVariables.indexOf(value) < 0)
                unifiedKeyValues.add(value);

        // insert projection operator, it is a *must*,
        // in hive, reduce sink sometimes also do the projection operator's
        // task
        currentOperator = new ProjectOperator(unifiedKeyValues);
        currentOperator.getInputs().add(parentOperator);
        parentOperator = new MutableObject<ILogicalOperator>(currentOperator);

        reduceSinkToKeyVariables.put(operator, keyVariables);
        List<String> fieldNames = new ArrayList<String>();
        List<TypeInfo> types = new ArrayList<TypeInfo>();
        for (LogicalVariable var : unifiedKeyValues) {
            fieldNames.add(var.toString());
            types.add(t.getType(var));
        }
        reduceSinkToFieldNames.put(operator, fieldNames);
        reduceSinkToTypes.put(operator, types);
        t.rewriteOperatorOutputSchema(variables, operator);

        latestAlgebricksOperator = currentOperator;
        latestReduceSink = operator;
        return new MutableObject<ILogicalOperator>(currentOperator);
    }

    /**
     * partial rewrite a join operator
     * 
     * @param operator
     * @param t
     */
    private void translateJoinOperatorPreprocess(Operator operator, Translator t) {
        JoinDesc desc = (JoinDesc) operator.getConf();
        ReduceSinkDesc reduceSinkDesc = (ReduceSinkDesc) latestReduceSink.getConf();
        int tag = reduceSinkDesc.getTag();

        Map<Byte, List<ExprNodeDesc>> exprMap = desc.getExprs();
        List<ExprNodeDesc> exprs = exprMap.get(Byte.valueOf((byte) tag));

        for (ExprNodeDesc expr : exprs)
            t.rewriteExpression(expr);

        List<Operator> parents = operatorToHiveParents.get(operator);
        if (parents == null) {
            parents = new ArrayList<Operator>();
            operatorToHiveParents.put(operator, parents);
        }
        parents.add(latestReduceSink);

        List<ILogicalOperator> asterixParents = operatorToAsterixParents.get(operator);
        if (asterixParents == null) {
            asterixParents = new ArrayList<ILogicalOperator>();
            operatorToAsterixParents.put(operator, asterixParents);
        }
        asterixParents.add(latestAlgebricksOperator);
    }

    // generate a join tree from a list of exchange/reducesink operator
    // both exchanges and reduce sinks have the same order
    private ILogicalOperator generateJoinTree(List<JoinCondDesc> conds, List<ILogicalOperator> exchanges,
            List<Operator> reduceSinks, int offset, Translator t) {
        // get a list of reduce sink descs (input descs)
        int inputSize = reduceSinks.size() - offset;

        if (inputSize == 2) {
            ILogicalOperator currentRoot;

            List<ReduceSinkDesc> reduceSinkDescs = new ArrayList<ReduceSinkDesc>();
            for (int i = reduceSinks.size() - 1; i >= offset; i--)
                reduceSinkDescs.add((ReduceSinkDesc) reduceSinks.get(i).getConf());

            // get the object inspector for the join
            List<String> fieldNames = new ArrayList<String>();
            List<TypeInfo> types = new ArrayList<TypeInfo>();
            for (int i = reduceSinks.size() - 1; i >= offset; i--) {
                fieldNames.addAll(reduceSinkToFieldNames.get(reduceSinks.get(i)));
                types.addAll(reduceSinkToTypes.get(reduceSinks.get(i)));
            }

            // get number of equality conjunctions in the final join condition
            int size = reduceSinkDescs.get(0).getKeyCols().size();

            // make up the join conditon expression
            List<ExprNodeDesc> joinConditionChildren = new ArrayList<ExprNodeDesc>();
            for (int i = 0; i < size; i++) {
                // create a join key pair
                List<ExprNodeDesc> keyPair = new ArrayList<ExprNodeDesc>();
                for (ReduceSinkDesc sink : reduceSinkDescs) {
                    keyPair.add(sink.getKeyCols().get(i));
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
                // there is no join equality condition, equal-join
                conjunct = new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, new Boolean(true));
            }
            // get an ILogicalExpression from hive's expression
            Mutable<ILogicalExpression> expression = t.translateScalarFucntion(conjunct);

            Mutable<ILogicalOperator> leftBranch = new MutableObject<ILogicalOperator>(
                    exchanges.get(exchanges.size() - 1));
            Mutable<ILogicalOperator> rightBranch = new MutableObject<ILogicalOperator>(
                    exchanges.get(exchanges.size() - 2));
            // get the join operator
            if (conds.get(offset).getType() == JoinDesc.LEFT_OUTER_JOIN) {
                currentRoot = new LeftOuterJoinOperator(expression);
                Mutable<ILogicalOperator> temp = leftBranch;
                leftBranch = rightBranch;
                rightBranch = temp;
            } else if (conds.get(offset).getType() == JoinDesc.RIGHT_OUTER_JOIN) {
                currentRoot = new LeftOuterJoinOperator(expression);
            } else
                currentRoot = new InnerJoinOperator(expression);

            currentRoot.getInputs().add(leftBranch);
            currentRoot.getInputs().add(rightBranch);

            // rewriteOperatorOutputSchema(variables, operator);
            return currentRoot;
        } else {
            // get the child join operator and insert and one-to-one exchange
            ILogicalOperator joinSrcOne = generateJoinTree(conds, exchanges, reduceSinks, offset + 1, t);
            // joinSrcOne.addInput(childJoin);

            ILogicalOperator currentRoot;

            List<ReduceSinkDesc> reduceSinkDescs = new ArrayList<ReduceSinkDesc>();
            for (int i = offset; i < offset + 2; i++)
                reduceSinkDescs.add((ReduceSinkDesc) reduceSinks.get(i).getConf());

            // get the object inspector for the join
            List<String> fieldNames = new ArrayList<String>();
            List<TypeInfo> types = new ArrayList<TypeInfo>();
            for (int i = offset; i < reduceSinks.size(); i++) {
                fieldNames.addAll(reduceSinkToFieldNames.get(reduceSinks.get(i)));
                types.addAll(reduceSinkToTypes.get(reduceSinks.get(i)));
            }

            // get number of equality conjunctions in the final join condition
            int size = reduceSinkDescs.get(0).getKeyCols().size();

            // make up the join condition expression
            List<ExprNodeDesc> joinConditionChildren = new ArrayList<ExprNodeDesc>();
            for (int i = 0; i < size; i++) {
                // create a join key pair
                List<ExprNodeDesc> keyPair = new ArrayList<ExprNodeDesc>();
                for (ReduceSinkDesc sink : reduceSinkDescs) {
                    keyPair.add(sink.getKeyCols().get(i));
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

            Mutable<ILogicalOperator> leftBranch = new MutableObject<ILogicalOperator>(joinSrcOne);
            Mutable<ILogicalOperator> rightBranch = new MutableObject<ILogicalOperator>(exchanges.get(offset));

            // get the join operator
            if (conds.get(offset).getType() == JoinDesc.LEFT_OUTER_JOIN) {
                currentRoot = new LeftOuterJoinOperator(expression);
                Mutable<ILogicalOperator> temp = leftBranch;
                leftBranch = rightBranch;
                rightBranch = temp;
            } else if (conds.get(offset).getType() == JoinDesc.RIGHT_OUTER_JOIN) {
                currentRoot = new LeftOuterJoinOperator(expression);
            } else
                currentRoot = new InnerJoinOperator(expression);

            // set the inputs from Algebricks join operator
            // add the current table
            currentRoot.getInputs().add(leftBranch);
            currentRoot.getInputs().add(rightBranch);

            return currentRoot;
        }
    }
}
