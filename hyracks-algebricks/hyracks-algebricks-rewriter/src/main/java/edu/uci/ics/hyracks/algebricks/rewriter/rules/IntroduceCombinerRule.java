package edu.uci.ics.hyracks.algebricks.rewriter.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;

public class IntroduceCombinerRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(LogicalOperatorReference opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(LogicalOperatorReference opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getOperator();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }
        context.addToDontApplySet(this, op);
        if (op.getOperatorTag() != LogicalOperatorTag.GROUP) {
            return false;
        }
        GroupByOperator gbyOp = (GroupByOperator) op;
        if (gbyOp.getExecutionMode() != ExecutionMode.PARTITIONED) {
            return false;
        }

        Map<AggregateFunctionCallExpression, SimilarAggregatesInfo> toReplaceMap = new HashMap<AggregateFunctionCallExpression, SimilarAggregatesInfo>();
        BookkeepingInfo bi = new BookkeepingInfo();
        bi.toReplaceMap = toReplaceMap;
        bi.modifGbyMap = new HashMap<GroupByOperator, List<LogicalVariable>>();

        GroupByOperator newGbyOp = opToPush(gbyOp, bi, context);
        if (newGbyOp == null) {
            return false;
        }

        for (Map.Entry<AggregateFunctionCallExpression, SimilarAggregatesInfo> entry : toReplaceMap.entrySet()) {
            SimilarAggregatesInfo sai = entry.getValue();
            for (AggregateExprInfo aei : sai.simAggs) {
                AbstractFunctionCallExpression afce = (AbstractFunctionCallExpression) aei.aggExprRef.getExpression();
                afce.setFunctionInfo(aei.newFunInfo);
                afce.getArguments().clear();
                afce.getArguments().add(new LogicalExpressionReference(sai.stepOneResult));
            }
        }

        for (Pair<LogicalVariable, LogicalExpressionReference> p : gbyOp.getDecorList()) {
            LogicalVariable newDecorVar = context.newVar();
            newGbyOp.addDecorExpression(newDecorVar, p.second.getExpression());
            p.second.setExpression(new VariableReferenceExpression(newDecorVar));
        }
        newGbyOp.setExecutionMode(ExecutionMode.LOCAL);
        Object v = gbyOp.getAnnotations().get(OperatorAnnotations.USE_HASH_GROUP_BY);
        newGbyOp.getAnnotations().put(OperatorAnnotations.USE_HASH_GROUP_BY, v);

        Object v2 = gbyOp.getAnnotations().get(OperatorAnnotations.USE_EXTERNAL_GROUP_BY);
        newGbyOp.getAnnotations().put(OperatorAnnotations.USE_EXTERNAL_GROUP_BY, v2);

        List<LogicalVariable> propagatedVars = new LinkedList<LogicalVariable>();
        VariableUtilities.getProducedVariables(newGbyOp, propagatedVars);

        Set<LogicalVariable> freeVars = new HashSet<LogicalVariable>();
        OperatorPropertiesUtil.getFreeVariablesInSubplans(gbyOp, freeVars);

        for (LogicalVariable var : freeVars) {
            if (!propagatedVars.contains(var)) {
                LogicalVariable newDecorVar = context.newVar();
                newGbyOp.addDecorExpression(newDecorVar, new VariableReferenceExpression(var));
                VariableUtilities.substituteVariables(gbyOp.getNestedPlans().get(0).getRoots().get(0).getOperator(),
                        var, newDecorVar, context);
            }
        }

        LogicalOperatorReference opRef3 = gbyOp.getInputs().get(0);
        opRef3.setOperator(newGbyOp);
        typeGby(newGbyOp, context);
        typeGby(gbyOp, context);
        return true;
    }

    private void typeGby(AbstractOperatorWithNestedPlans op, IOptimizationContext context) throws AlgebricksException {
        for (ILogicalPlan p : op.getNestedPlans()) {
            OperatorPropertiesUtil.typePlan(p, context);
        }
        context.computeAndSetTypeEnvironmentForOperator(op);
    }

    private GroupByOperator opToPush(GroupByOperator gbyOp, BookkeepingInfo bi, IOptimizationContext context)
            throws AlgebricksException {

        LogicalOperatorReference opRef3 = gbyOp.getInputs().get(0);
        ILogicalOperator op3 = opRef3.getOperator();
        GroupByOperator newGbyOp = new GroupByOperator();
        newGbyOp.getInputs().add(new LogicalOperatorReference(op3));
        // copy annotations
        Map<String, Object> annotations = newGbyOp.getAnnotations();
        for (Entry<String, Object> a : gbyOp.getAnnotations().entrySet())
            annotations.put(a.getKey(), a.getValue());

        List<LogicalVariable> gbyVars = gbyOp.getGbyVarList();

        for (ILogicalPlan p : gbyOp.getNestedPlans()) {
            Pair<Boolean, ILogicalPlan> bip = tryToPushSubplan(p, gbyOp, newGbyOp, bi, gbyVars, context);
            if (!bip.first) {
                // for now, if we cannot push everything, give up
                return null;
            }
            ILogicalPlan pushedSubplan = bip.second;
            if (pushedSubplan != null) {
                newGbyOp.getNestedPlans().add(pushedSubplan);
            }
        }

        ArrayList<LogicalVariable> newOpGbyList = new ArrayList<LogicalVariable>();
        ArrayList<LogicalVariable> replGbyList = new ArrayList<LogicalVariable>();
        // find maximal sequence of variable
        for (Map.Entry<GroupByOperator, List<LogicalVariable>> e : bi.modifGbyMap.entrySet()) {
            List<LogicalVariable> varList = e.getValue();
            boolean see1 = true;
            int sz1 = newOpGbyList.size();
            int i = 0;
            for (LogicalVariable v : varList) {
                if (see1) {
                    if (i < sz1) {
                        LogicalVariable v2 = newOpGbyList.get(i);
                        if (v != v2) {
                            // cannot linearize
                            return null;
                        }
                    } else {
                        see1 = false;
                        newOpGbyList.add(v);
                        replGbyList.add(context.newVar());
                    }
                    i++;
                } else {
                    newOpGbyList.add(v);
                    replGbyList.add(context.newVar());
                }
            }
        }
        // set the vars in the new op
        int n = newOpGbyList.size();
        for (int i = 0; i < n; i++) {
            newGbyOp.addGbyExpression(replGbyList.get(i), new VariableReferenceExpression(newOpGbyList.get(i)));
            VariableUtilities.substituteVariables(gbyOp, newOpGbyList.get(i), replGbyList.get(i), false, context);
        }
        return newGbyOp;
    }

    private Pair<Boolean, ILogicalPlan> tryToPushSubplan(ILogicalPlan p, GroupByOperator oldGbyOp,
            GroupByOperator newGbyOp, BookkeepingInfo bi, List<LogicalVariable> gbyVars, IOptimizationContext context) {
        List<LogicalOperatorReference> pushedRoots = new ArrayList<LogicalOperatorReference>();
        List<LogicalOperatorReference> toPushR = new ArrayList<LogicalOperatorReference>();
        for (LogicalOperatorReference r : p.getRoots()) {
            if (!tryToPushRoot(r, oldGbyOp, newGbyOp, bi, gbyVars, context, toPushR)) {
                // for now, if we cannot push everything, give up
                return new Pair<Boolean, ILogicalPlan>(false, null);
            }
        }
        for (LogicalOperatorReference root : toPushR) {
            pushedRoots.add(root);
        }
        if (pushedRoots.isEmpty()) {
            return new Pair<Boolean, ILogicalPlan>(true, null);
        } else {
            return new Pair<Boolean, ILogicalPlan>(true, new ALogicalPlanImpl(pushedRoots));
        }
    }

    private boolean tryToPushRoot(LogicalOperatorReference r, GroupByOperator oldGbyOp, GroupByOperator newGbyOp,
            BookkeepingInfo bi, List<LogicalVariable> gbyVars, IOptimizationContext context,
            List<LogicalOperatorReference> toPushAccumulate) {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) r.getOperator();
        if (op1.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getOperator();
        if (op2.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            AggregateOperator initAgg = (AggregateOperator) op1;
            Pair<Boolean, LogicalOperatorReference> pOpRef = tryToPushAgg(initAgg, newGbyOp, bi.toReplaceMap, context);
            if (!pOpRef.first) {
                return false;
            }
            LogicalOperatorReference opRef = pOpRef.second;
            if (opRef != null) {
                toPushAccumulate.add(opRef);
            }
            bi.modifGbyMap.put(oldGbyOp, gbyVars);
            return true;
        } else {
            while (op2.getOperatorTag() != LogicalOperatorTag.GROUP && op2.getInputs().size() == 1) {
                op2 = (AbstractLogicalOperator) op2.getInputs().get(0).getOperator();
            }
            if (op2.getOperatorTag() != LogicalOperatorTag.GROUP) {
                return false;
            }
            GroupByOperator nestedGby = (GroupByOperator) op2;
            List<LogicalVariable> gbyVars2 = nestedGby.getGbyVarList();
            List<LogicalVariable> concatGbyVars = new ArrayList<LogicalVariable>(gbyVars);
            concatGbyVars.addAll(gbyVars2);
            for (ILogicalPlan p : nestedGby.getNestedPlans()) {
                for (LogicalOperatorReference r2 : p.getRoots()) {
                    if (!tryToPushRoot(r2, nestedGby, newGbyOp, bi, concatGbyVars, context, toPushAccumulate)) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    private Pair<Boolean, LogicalOperatorReference> tryToPushAgg(AggregateOperator initAgg, GroupByOperator newGbyOp,
            Map<AggregateFunctionCallExpression, SimilarAggregatesInfo> toReplaceMap, IOptimizationContext context) {

        ArrayList<LogicalVariable> pushedVars = new ArrayList<LogicalVariable>();
        ArrayList<LogicalExpressionReference> pushedExprs = new ArrayList<LogicalExpressionReference>();

        List<LogicalVariable> initVars = initAgg.getVariables();
        List<LogicalExpressionReference> initExprs = initAgg.getExpressions();
        int sz = initVars.size();
        for (int i = 0; i < sz; i++) {
            AggregateFunctionCallExpression aggFun = (AggregateFunctionCallExpression) initExprs.get(i).getExpression();
            if (!aggFun.isTwoStep()) {
                return new Pair<Boolean, LogicalOperatorReference>(false, null);
            }
        }

        boolean haveAggToReplace = false;
        for (int i = 0; i < sz; i++) {
            LogicalExpressionReference expRef = initExprs.get(i);
            AggregateFunctionCallExpression aggFun = (AggregateFunctionCallExpression) expRef.getExpression();
            IFunctionInfo fi1 = aggFun.getStepOneAggregate();
            List<LogicalExpressionReference> newArgs = new ArrayList<LogicalExpressionReference>(aggFun.getArguments()
                    .size());
            for (LogicalExpressionReference er : aggFun.getArguments()) {
                newArgs.add(new LogicalExpressionReference(er.getExpression().cloneExpression()));
            }
//            AggregateFunctionCallExpression aggLocal = new AggregateFunctionCallExpression(fi1, false, newArgs);
//            pushedExprs.add(new LogicalExpressionReference(aggLocal));

            IFunctionInfo fi2 = aggFun.getStepTwoAggregate();

            SimilarAggregatesInfo inf = toReplaceMap.get(aggFun);
            if (inf == null) {
                inf = new SimilarAggregatesInfo();
                LogicalVariable newAggVar = context.newVar();
                pushedVars.add(newAggVar);
                inf.stepOneResult = new VariableReferenceExpression(newAggVar);
                inf.simAggs = new ArrayList<AggregateExprInfo>();
                toReplaceMap.put(aggFun, inf);
                AggregateFunctionCallExpression aggLocal = new AggregateFunctionCallExpression(fi1, false, newArgs);
                pushedExprs.add(new LogicalExpressionReference(aggLocal));
            }
            AggregateExprInfo aei = new AggregateExprInfo();
            aei.aggExprRef = expRef;
            aei.newFunInfo = fi2;
            inf.simAggs.add(aei);
            haveAggToReplace = true;
        }

        if (!pushedVars.isEmpty()) {
            AggregateOperator pushedAgg = new AggregateOperator(pushedVars, pushedExprs);
            pushedAgg.setExecutionMode(ExecutionMode.LOCAL);
            NestedTupleSourceOperator nts = new NestedTupleSourceOperator(new LogicalOperatorReference(newGbyOp));
            nts.setExecutionMode(ExecutionMode.LOCAL);
            pushedAgg.getInputs().add(new LogicalOperatorReference(nts));
            return new Pair<Boolean, LogicalOperatorReference>(true, new LogicalOperatorReference(pushedAgg));
        } else {
            return new Pair<Boolean, LogicalOperatorReference>(haveAggToReplace, null);
        }
    }

    private class SimilarAggregatesInfo {
        ILogicalExpression stepOneResult;
        List<AggregateExprInfo> simAggs;
    }

    private class AggregateExprInfo {
        LogicalExpressionReference aggExprRef;
        IFunctionInfo newFunInfo;
    }

    private class BookkeepingInfo {
        Map<AggregateFunctionCallExpression, SimilarAggregatesInfo> toReplaceMap;
        Map<GroupByOperator, List<LogicalVariable>> modifGbyMap;
    }
}
