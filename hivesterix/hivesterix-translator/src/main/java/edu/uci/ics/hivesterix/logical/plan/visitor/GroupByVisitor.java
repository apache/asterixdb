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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import edu.uci.ics.hivesterix.common.config.ConfUtil;
import edu.uci.ics.hivesterix.logical.plan.HiveOperatorAnnotations;
import edu.uci.ics.hivesterix.logical.plan.visitor.base.DefaultVisitor;
import edu.uci.ics.hivesterix.logical.plan.visitor.base.Translator;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class GroupByVisitor extends DefaultVisitor {

    private List<Mutable<ILogicalExpression>> AlgebricksAggs = new ArrayList<Mutable<ILogicalExpression>>();
    private List<IFunctionInfo> localAggs = new ArrayList<IFunctionInfo>();
    private boolean isDistinct = false;
    private boolean gbyKeyNotRedKey = false;

    @Override
    public Mutable<ILogicalOperator> visit(GroupByOperator operator,
            Mutable<ILogicalOperator> AlgebricksParentOperatorRef, Translator t) throws AlgebricksException {

        // get descriptors
        GroupByDesc desc = (GroupByDesc) operator.getConf();
        GroupByDesc.Mode mode = desc.getMode();

        List<ExprNodeDesc> keys = desc.getKeys();
        List<AggregationDesc> aggregators = desc.getAggregators();

        Operator child = operator.getChildOperators().get(0);

        if (child.getType() == OperatorType.REDUCESINK) {
            List<ExprNodeDesc> partKeys = ((ReduceSinkDesc) child.getConf()).getPartitionCols();
            if (keys.size() != partKeys.size())
                gbyKeyNotRedKey = true;
        }

        if (mode == GroupByDesc.Mode.PARTIAL1 || mode == GroupByDesc.Mode.HASH || mode == GroupByDesc.Mode.COMPLETE
                || (aggregators.size() == 0 && isDistinct == false) || gbyKeyNotRedKey) {
            AlgebricksAggs.clear();
            // add an assign operator if the key is not a column expression
            ArrayList<LogicalVariable> keyVariables = new ArrayList<LogicalVariable>();
            ILogicalOperator currentOperator = null;
            ILogicalOperator assignOperator = t.getAssignOperator(AlgebricksParentOperatorRef, keys, keyVariables);
            if (assignOperator != null) {
                currentOperator = assignOperator;
                AlgebricksParentOperatorRef = new MutableObject<ILogicalOperator>(currentOperator);
            }

            // get key variable expression list
            List<Mutable<ILogicalExpression>> keyExprs = new ArrayList<Mutable<ILogicalExpression>>();
            for (LogicalVariable var : keyVariables) {
                keyExprs.add(t.translateScalarFucntion(new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, var
                        .toString(), "", false)));
            }

            if (aggregators.size() == 0) {
                List<Mutable<ILogicalExpression>> distinctExprs = new ArrayList<Mutable<ILogicalExpression>>();
                for (LogicalVariable var : keyVariables) {
                    Mutable<ILogicalExpression> varExpr = new MutableObject<ILogicalExpression>(
                            new VariableReferenceExpression(var));
                    distinctExprs.add(varExpr);
                }
                t.rewriteOperatorOutputSchema(keyVariables, operator);
                isDistinct = true;
                ILogicalOperator lop = new DistinctOperator(distinctExprs);
                lop.getInputs().add(AlgebricksParentOperatorRef);
                return new MutableObject<ILogicalOperator>(lop);
            }

            // get the pair<LogicalVariable, ILogicalExpression> list
            List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> keyParameters = new ArrayList<Pair<LogicalVariable, Mutable<ILogicalExpression>>>();
            keyVariables.clear();
            for (Mutable<ILogicalExpression> expr : keyExprs) {
                LogicalVariable keyVar = t.getVariable(expr.getValue().toString(), TypeInfoFactory.unknownTypeInfo);
                keyParameters.add(new Pair(keyVar, expr));
                keyVariables.add(keyVar);
            }

            // get the parameters for the aggregator operator
            ArrayList<LogicalVariable> aggVariables = new ArrayList<LogicalVariable>();
            ArrayList<Mutable<ILogicalExpression>> aggExprs = new ArrayList<Mutable<ILogicalExpression>>();

            // get the type of each aggregation function
            HashMap<AggregationDesc, TypeInfo> aggToType = new HashMap<AggregationDesc, TypeInfo>();
            List<ColumnInfo> columns = operator.getSchema().getSignature();
            int offset = keys.size();
            for (int i = offset; i < columns.size(); i++) {
                aggToType.put(aggregators.get(i - offset), columns.get(i).getType());
            }

            localAggs.clear();
            // rewrite parameter expressions for all aggregators
            for (AggregationDesc aggregator : aggregators) {
                for (ExprNodeDesc parameter : aggregator.getParameters()) {
                    t.rewriteExpression(parameter);
                }
                Mutable<ILogicalExpression> aggExpr = t.translateAggregation(aggregator);
                AbstractFunctionCallExpression localAggExpr = (AbstractFunctionCallExpression) aggExpr.getValue();
                localAggs.add(localAggExpr.getFunctionInfo());

                AggregationDesc logicalAgg = new AggregationDesc(aggregator.getGenericUDAFName(),
                        aggregator.getGenericUDAFEvaluator(), aggregator.getParameters(), aggregator.getDistinct(),
                        Mode.COMPLETE);
                Mutable<ILogicalExpression> logicalAggExpr = t.translateAggregation(logicalAgg);

                AlgebricksAggs.add(logicalAggExpr);
                if (!gbyKeyNotRedKey)
                    aggExprs.add(logicalAggExpr);
                else
                    aggExprs.add(aggExpr);

                aggVariables.add(t.getVariable(aggregator.getExprString() + aggregator.getMode(),
                        aggToType.get(aggregator)));
            }

            if (child.getType() != OperatorType.REDUCESINK)
                gbyKeyNotRedKey = false;

            // get the sub plan list
            AggregateOperator aggOperator = new AggregateOperator(aggVariables, aggExprs);
            NestedTupleSourceOperator nestedTupleSource = new NestedTupleSourceOperator(
                    new MutableObject<ILogicalOperator>());
            aggOperator.getInputs().add(new MutableObject<ILogicalOperator>(nestedTupleSource));

            List<Mutable<ILogicalOperator>> subRoots = new ArrayList<Mutable<ILogicalOperator>>();
            subRoots.add(new MutableObject<ILogicalOperator>(aggOperator));
            ILogicalPlan subPlan = new ALogicalPlanImpl(subRoots);
            List<ILogicalPlan> subPlans = new ArrayList<ILogicalPlan>();
            subPlans.add(subPlan);

            // create the group by operator
            currentOperator = new edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator(
                    keyParameters, new ArrayList<Pair<LogicalVariable, Mutable<ILogicalExpression>>>(), subPlans);
            currentOperator.getInputs().add(AlgebricksParentOperatorRef);
            nestedTupleSource.getDataSourceReference().setValue(currentOperator);

            List<LogicalVariable> outputVariables = new ArrayList<LogicalVariable>();
            outputVariables.addAll(keyVariables);
            outputVariables.addAll(aggVariables);
            t.rewriteOperatorOutputSchema(outputVariables, operator);

            if (gbyKeyNotRedKey) {
                currentOperator.getAnnotations().put(HiveOperatorAnnotations.LOCAL_GROUP_BY, Boolean.TRUE);
            }

            HiveConf conf = ConfUtil.getHiveConf();
            Boolean extGby = conf.getBoolean("hive.algebricks.groupby.external", false);

            if (extGby && isSerializable(aggregators)) {
                currentOperator.getAnnotations().put(OperatorAnnotations.USE_EXTERNAL_GROUP_BY, Boolean.TRUE);
            }
            return new MutableObject<ILogicalOperator>(currentOperator);
        } else {
            isDistinct = false;
            // rewrite parameter expressions for all aggregators
            int i = 0;
            for (AggregationDesc aggregator : aggregators) {
                for (ExprNodeDesc parameter : aggregator.getParameters()) {
                    t.rewriteExpression(parameter);
                }
                Mutable<ILogicalExpression> agg = t.translateAggregation(aggregator);
                AggregateFunctionCallExpression originalAgg = (AggregateFunctionCallExpression) AlgebricksAggs.get(i)
                        .getValue();
                originalAgg.setStepOneAggregate(localAggs.get(i));
                AggregateFunctionCallExpression currentAgg = (AggregateFunctionCallExpression) agg.getValue();
                if (currentAgg.getFunctionInfo() != null) {
                    originalAgg.setTwoStep(true);
                    originalAgg.setStepTwoAggregate(currentAgg.getFunctionInfo());
                }
                i++;
            }
            return null;
        }
    }

    @Override
    public Mutable<ILogicalOperator> visit(ReduceSinkOperator operator,
            Mutable<ILogicalOperator> AlgebricksParentOperatorRef, Translator t) {
        Operator downStream = (Operator) operator.getChildOperators().get(0);
        if (!(downStream instanceof GroupByOperator)) {
            return null;
        }

        ReduceSinkDesc desc = (ReduceSinkDesc) operator.getConf();
        List<ExprNodeDesc> keys = desc.getKeyCols();
        List<ExprNodeDesc> values = desc.getValueCols();

        // insert assign for keys
        ArrayList<LogicalVariable> keyVariables = new ArrayList<LogicalVariable>();
        t.getAssignOperator(AlgebricksParentOperatorRef, keys, keyVariables);

        // insert assign for values
        ArrayList<LogicalVariable> valueVariables = new ArrayList<LogicalVariable>();
        t.getAssignOperator(AlgebricksParentOperatorRef, values, valueVariables);

        ArrayList<LogicalVariable> columns = new ArrayList<LogicalVariable>();
        columns.addAll(keyVariables);
        columns.addAll(valueVariables);

        t.rewriteOperatorOutputSchema(columns, operator);
        return null;
    }

    private boolean isSerializable(List<AggregationDesc> descs) throws AlgebricksException {
        try {
            for (AggregationDesc desc : descs) {
                GenericUDAFEvaluator udaf = desc.getGenericUDAFEvaluator();
                AggregationBuffer buf = udaf.getNewAggregationBuffer();
                Class<?> bufferClass = buf.getClass();
                Field[] fields = bufferClass.getDeclaredFields();
                for (Field field : fields) {
                    field.setAccessible(true);
                    String type = field.getType().toString();
                    if (!(type.equals("int") || type.equals("long") || type.equals("float") || type.equals("double") || type
                            .equals("boolean"))) {
                        return false;
                    }
                }

            }
            return true;
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

}
