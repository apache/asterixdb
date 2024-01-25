/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.optimizer.rules.cbo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DistinctOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class OperatorUtils {

    public static void createDistinctOpsForJoinNodes(ILogicalOperator op,
            Pair<List<LogicalVariable>, List<AbstractFunctionCallExpression>> distinctVarsFuncPair,
            IOptimizationContext context, HashMap<DataSourceScanOperator, ILogicalOperator> scanAndDistinctOps) {

        List<LogicalVariable> distinctVars = distinctVarsFuncPair.getFirst();
        List<AbstractFunctionCallExpression> distinctFunctions = distinctVarsFuncPair.getSecond();
        if (op == null || distinctVars.size() == 0) {
            return;
        }

        List<LogicalVariable> foundDistinctVars = new ArrayList<>();
        ILogicalOperator selOp = null, assignOp = null;

        ILogicalOperator currentOp = op;
        LogicalOperatorTag tag = currentOp.getOperatorTag();
        // add DistinctOp to count distinct values in an attribute
        if (tag == LogicalOperatorTag.INNERJOIN || tag == LogicalOperatorTag.LEFTOUTERJOIN) {
            for (int i = 0; i < currentOp.getInputs().size(); i++) {
                ILogicalOperator nextOp = currentOp.getInputs().get(i).getValue();
                createDistinctOpsForJoinNodes(nextOp, distinctVarsFuncPair, context, scanAndDistinctOps);
            }
        } else {
            DataSourceScanOperator scanOp = null;
            LogicalVariable assignVar;
            while (tag != LogicalOperatorTag.EMPTYTUPLESOURCE) {
                if (tag == LogicalOperatorTag.SELECT) {
                    ILogicalOperator nextOp = currentOp.getInputs().get(0).getValue();
                    if (nextOp.getOperatorTag() == LogicalOperatorTag.ASSIGN
                            || nextOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                        if (selOp == null && assignOp == null) { // first corresponding SelectOp found iff no corresponding AssignOp appeared before
                            selOp = currentOp; // one DataSourceScanOp possible, save the corresponding SelectOp
                        }
                    }
                } else if (tag == LogicalOperatorTag.ASSIGN) {
                    assignVar = ((AssignOperator) currentOp).getVariables().get(0);
                    int idx = distinctVars.indexOf(assignVar);
                    if (idx != -1 && assignOp == null) { // first corresponding AssignOp found
                        assignOp = currentOp;
                    }
                    if (idx != -1) { // add all variables of the AssignOp
                        foundDistinctVars.add(assignVar);
                    }
                } else if (tag == LogicalOperatorTag.DATASOURCESCAN) {
                    scanOp = (DataSourceScanOperator) currentOp;
                    // will work for any attributes present in GroupByOp or DistinctOp
                    List<LogicalVariable> scanVars = scanOp.getVariables();
                    for (LogicalVariable scanVar : scanVars) { // add all required variables of the DataSourceScanOp
                        if (distinctVars.contains(scanVar)) {
                            foundDistinctVars.add(scanVar);
                        }
                    }
                    if (foundDistinctVars.size() == 0) {
                        scanOp = null; // GroupByOp or DistinctOp doesn't contain any attributes of the dataset
                    }
                } else if (tag == LogicalOperatorTag.GROUP) { // GroupByOp found through looping (not as direct inputs of a JoinOp)
                    List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> nestedGrpVarsList =
                            ((GroupByOperator) currentOp).getGroupByList();
                    // look for any DistinctOp/GroupByOp variables are replaceable with a nested GroupByOp Variable-expression
                    for (int i = 0; i < nestedGrpVarsList.size(); i++) {
                        LogicalVariable prevVar = nestedGrpVarsList.get(i).first;
                        int idx = distinctVars.indexOf(prevVar);
                        if (idx != -1 && distinctVars.size() > 0) {
                            ILogicalExpression expr = nestedGrpVarsList.get(i).second.getValue();
                            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                                LogicalVariable newVar = ((VariableReferenceExpression) expr).getVariableReference();
                                distinctVars.remove(idx);
                                distinctVars.add(newVar);

                                // replace corresponding functions' variables
                                for (AbstractFunctionCallExpression funcExpr : distinctFunctions) {
                                    replaceVariableInFunction(funcExpr, prevVar, newVar);
                                }
                            }
                        }
                    }
                } else if (tag == LogicalOperatorTag.INNERJOIN || tag == LogicalOperatorTag.LEFTOUTERJOIN) {
                    for (int i = 0; i < currentOp.getInputs().size(); i++) {
                        ILogicalOperator nextOp = currentOp.getInputs().get(i).getValue();
                        createDistinctOpsForJoinNodes(nextOp, distinctVarsFuncPair, context, scanAndDistinctOps);
                    }
                    break; // next operators are already handled in the recursion, so exit looping
                }
                // TODO(mehnaz): handle DISTINCT and UNNEST operators (if appears in sub-queries)

                // proceed to the next operator
                currentOp = currentOp.getInputs().get(0).getValue();
                tag = currentOp.getOperatorTag();
            }

            if (scanOp != null) {
                ILogicalOperator inputOp = (selOp != null) ? selOp : ((assignOp != null) ? assignOp : scanOp);
                SourceLocation sourceLocation = inputOp.getSourceLocation();
                DistinctOperator distinctOp =
                        createDistinctOp(foundDistinctVars, inputOp, sourceLocation, distinctFunctions, context);
                if (distinctOp != null) {
                    scanAndDistinctOps.put(scanOp, distinctOp);
                }
            }
        }
    }

    private static void replaceVariableInFunction(AbstractFunctionCallExpression funcExpr, LogicalVariable prevVar,
            LogicalVariable newVar) {
        List<Mutable<ILogicalExpression>> argList = funcExpr.getArguments();
        for (Mutable<ILogicalExpression> arg : argList) {
            if (arg.getValue().getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                if (prevVar.equals(((VariableReferenceExpression) arg.getValue()).getVariableReference())) {
                    arg.getValue().substituteVar(prevVar, newVar);
                }
            } else if (arg.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                replaceVariableInFunction((AbstractFunctionCallExpression) arg.getValue(), prevVar, newVar);
            }
        }
    }

    private static List<LogicalVariable> getFunctionVariables(AbstractFunctionCallExpression funcExpr) {
        List<LogicalVariable> variables = new ArrayList<>();
        List<Mutable<ILogicalExpression>> argList = funcExpr.getArguments();
        for (Mutable<ILogicalExpression> arg : argList) {
            if (arg.getValue().getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                variables.add(((VariableReferenceExpression) arg.getValue()).getVariableReference());
            } else if (arg.getValue().getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                variables.addAll(getFunctionVariables((AbstractFunctionCallExpression) arg.getValue()));
            }
        }
        return variables;
    }

    public static Pair<List<LogicalVariable>, List<AbstractFunctionCallExpression>> getGroupByDistinctVarFuncPair(
            ILogicalOperator grpByDistinctOp) {
        Pair<List<LogicalVariable>, List<AbstractFunctionCallExpression>> distinctVarsFunctions =
                new Pair<>(new ArrayList<>(), new ArrayList<>());
        List<LogicalVariable> distinctVars = distinctVarsFunctions.getFirst();
        List<AbstractFunctionCallExpression> distinctFunctions = distinctVarsFunctions.getSecond();

        if (grpByDistinctOp == null) {
            return distinctVarsFunctions;
        }

        ILogicalExpression varRef;
        ILogicalOperator nextOp;
        if (grpByDistinctOp.getOperatorTag() == LogicalOperatorTag.DISTINCT) {
            ILogicalExpression distinctExpr = ((DistinctOperator) grpByDistinctOp).getExpressions().get(0).getValue();
            if (distinctExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) { // this Distinct expression should be a variable
                distinctVars.add(((VariableReferenceExpression) distinctExpr).getVariableReference()); // initial Variable-expression
                nextOp = grpByDistinctOp.getInputs().get(0).getValue();

                // loop through as long as AssignOp variable does not match with DistinctOp Variable-expression
                while (nextOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                    LogicalVariable assignVar = ((AssignOperator) nextOp).getVariables().get(0);
                    if (assignVar.equals(((VariableReferenceExpression) distinctExpr).getVariableReference())) {
                        assert distinctVars.size() == 1;
                        distinctVars.remove(0); // remove initial Variable-expression of DistinctOp

                        ILogicalExpression assignExpr = ((AssignOperator) nextOp).getExpressions().get(0).getValue();
                        if (assignExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) { // FId: open-object-constructor
                            List<Mutable<ILogicalExpression>> argList =
                                    ((AbstractFunctionCallExpression) assignExpr).getArguments();

                            // add all variables and the corresponding functions from AssignOp arguments
                            for (int i = 0; i < argList.size(); i += 2) {
                                varRef = argList.get(i + 1).getValue();
                                if (varRef.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                                    distinctVars.add(((VariableReferenceExpression) varRef).getVariableReference());
                                } else if (varRef.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                                    distinctVars.addAll(getFunctionVariables((AbstractFunctionCallExpression) varRef));
                                    distinctFunctions.add((AbstractFunctionCallExpression) varRef.cloneExpression());
                                }
                            }
                        }
                        break;
                    }
                    nextOp = nextOp.getInputs().get(0).getValue();
                }
            }
        } else if (grpByDistinctOp.getOperatorTag() == LogicalOperatorTag.GROUP) {
            distinctVars.addAll(((GroupByOperator) grpByDistinctOp).getGroupByVarList());
            nextOp = grpByDistinctOp.getInputs().get(0).getValue();
            LogicalOperatorTag tag = nextOp.getOperatorTag();
            while (tag != LogicalOperatorTag.DATASOURCESCAN) {
                if (tag == LogicalOperatorTag.INNERJOIN || tag == LogicalOperatorTag.LEFTOUTERJOIN) {
                    break;
                } else if (tag == LogicalOperatorTag.ASSIGN) {
                    ILogicalExpression assignExpr = ((AssignOperator) nextOp).getExpressions().get(0).getValue();
                    if (assignExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                        List<LogicalVariable> fVars = getFunctionVariables((AbstractFunctionCallExpression) assignExpr);
                        LogicalVariable assignVar = ((AssignOperator) nextOp).getVariables().get(0);
                        int idx = distinctVars.indexOf(assignVar);
                        if (idx != -1 && fVars.size() > 0) {
                            distinctVars.remove(idx);
                            distinctVars.addAll(fVars);
                            distinctFunctions.add((AbstractFunctionCallExpression) assignExpr.cloneExpression());
                        }
                    }
                }
                nextOp = nextOp.getInputs().get(0).getValue();
                tag = nextOp.getOperatorTag();
            }
        }
        return distinctVarsFunctions;
    }

    private static AssignOperator createAssignOpForFunctionExpr(IOptimizationContext optCtx,
            List<LogicalVariable> distinctVars, List<AbstractFunctionCallExpression> funcExpr,
            SourceLocation sourceLocation) {
        int counter = 1;
        List<LogicalVariable> notFoundDistinctVars = new ArrayList<>(distinctVars);
        List<Mutable<ILogicalExpression>> openRecConsArgs = new ArrayList<>();
        for (AbstractFunctionCallExpression expr : funcExpr) {
            List<LogicalVariable> funcVars = getFunctionVariables(expr);
            if (new HashSet<>(distinctVars).containsAll(funcVars)) { // all variables in the function are of the current dataset
                openRecConsArgs.add(new MutableObject<>(
                        new ConstantExpression(new AsterixConstantValue(new AString(String.valueOf(counter))))));
                openRecConsArgs.add(new MutableObject<>(expr));
                counter++;
                notFoundDistinctVars.removeAll(funcVars); // DistinctOp variables are found in the function, so remove
            }
        }
        if (openRecConsArgs.size() > 0) { // at least one Function expression is available/applicable
            for (LogicalVariable var : notFoundDistinctVars) {
                openRecConsArgs.add(new MutableObject<>(
                        new ConstantExpression(new AsterixConstantValue(new AString(String.valueOf(counter))))));
                openRecConsArgs.add(new MutableObject<>(new VariableReferenceExpression(var)));
                counter++;
            }
            AbstractFunctionCallExpression openRecFunc = new ScalarFunctionCallExpression(
                    BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR), openRecConsArgs);
            LogicalVariable assignVar = optCtx.newVar();
            AssignOperator assignOp = new AssignOperator(assignVar, new MutableObject<>(openRecFunc));
            assignOp.setSourceLocation(sourceLocation);
            return assignOp;
        }
        return null;
    }

    private static DistinctOperator createDistinctOp(List<LogicalVariable> distinctVars, ILogicalOperator inputOp,
            SourceLocation sourceLocation, List<AbstractFunctionCallExpression> funcExpr, IOptimizationContext optCtx) {
        if (distinctVars.size() == 0 || inputOp == null) {
            return null;
        }
        LogicalOperatorTag tag = inputOp.getOperatorTag();
        if (tag != LogicalOperatorTag.ASSIGN && tag != LogicalOperatorTag.SELECT
                && tag != LogicalOperatorTag.DATASOURCESCAN) {
            return null;
        }

        // create an AssignOp for Function expressions of the corresponding GroupByOp or DistinctOp
        AssignOperator assignOp = createAssignOpForFunctionExpr(optCtx, distinctVars, funcExpr, sourceLocation);

        List<Mutable<ILogicalExpression>> distinctExpr = new ArrayList<>();
        if (assignOp == null) { // no Function expressions are available/applicable for the new DistinctOp
            for (LogicalVariable var : distinctVars) {
                VariableReferenceExpression varExpr = new VariableReferenceExpression(var);
                varExpr.setSourceLocation(sourceLocation);
                distinctExpr.add(new MutableObject<>(varExpr));
            }
        } else {
            VariableReferenceExpression varExpr = new VariableReferenceExpression(assignOp.getVariables().get(0));
            varExpr.setSourceLocation(sourceLocation);
            distinctExpr.add(new MutableObject<>(varExpr));
        }

        // create a new Distinct operator
        DistinctOperator distinctOp = new DistinctOperator(distinctExpr);
        distinctOp.setSourceLocation(sourceLocation);
        if (assignOp == null) {
            distinctOp.getInputs().add(new MutableObject<>(inputOp));
        } else {
            distinctOp.getInputs().add(new MutableObject<>(assignOp));
            ILogicalOperator nextOp = distinctOp.getInputs().get(0).getValue();
            nextOp.getInputs().add(new MutableObject<>(inputOp));
        }
        distinctOp.setExecutionMode(inputOp.getExecutionMode());

        return distinctOp;
    }
}
