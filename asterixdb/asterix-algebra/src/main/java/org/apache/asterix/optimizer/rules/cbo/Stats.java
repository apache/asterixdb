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
import java.util.List;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DataSourceId;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.JoinProductivityAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.PredicateCardinalityAnnotation;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;

public class Stats {

    public double SELECTIVITY_FOR_SECONDARY_INDEX_SELECTION = 0.1;

    protected IOptimizationContext optCtx;
    protected JoinEnum joinEnum;

    public Stats(IOptimizationContext context, JoinEnum joinE) {
        optCtx = context;
        joinEnum = joinE;
    }

    public DataverseName findDataverseName(DataSourceScanOperator scanOp) {
        if (scanOp == null) {
            // this should rarely happen (IN lists may cause this)
            return null;
        }
        DataSourceId dsid = (DataSourceId) scanOp.getDataSource().getId();
        return dsid.getDataverseName();
    }

    public Index findSampleIndex(DataSourceScanOperator scanOp, IOptimizationContext context)
            throws AlgebricksException {
        DataverseName dataverseName = findDataverseName(scanOp);
        DataSource ds = (DataSource) scanOp.getDataSource();
        DataSourceId dsid = ds.getId();
        MetadataProvider mdp = (MetadataProvider) context.getMetadataProvider();
        return mdp.findSampleIndex(dataverseName, dsid.getDatasourceName());
    }

    private double findJoinSelectivity(JoinProductivityAnnotation anno, AbstractFunctionCallExpression joinExpr)
            throws AlgebricksException {
        List<LogicalVariable> exprUsedVars = new ArrayList<>();
        joinExpr.getUsedVariables(exprUsedVars);
        if (exprUsedVars.size() != 2) {
            // Since there is a left and right dataset here, expecting only two variables.
            return 1.0;
        }
        int idx1 = joinEnum.findJoinNodeIndex(exprUsedVars.get(0)) + 1;
        int idx2 = joinEnum.findJoinNodeIndex(exprUsedVars.get(1)) + 1;
        double card1 = joinEnum.getJnArray()[idx1].origCardinality;
        double card2 = joinEnum.getJnArray()[idx2].origCardinality;
        if (card1 == 0.0 || card2 == 0.0) // should not happen
        {
            return 1.0;
        }

        // join sel  = leftside * productivity/(card1 * card2);
        if (anno != null) {
            int leftIndex = joinEnum.findJoinNodeIndexByName(anno.getLeftSideDataSet());
            if (leftIndex != idx1 && leftIndex != idx2) {
                // should not happen
                return 1.0;
            }
            if (leftIndex == idx1) {
                return anno.getJoinProductivity() / card2;
            } else {
                return anno.getJoinProductivity() / card1;
            }
        } else {
            if (card1 < card2) {
                // we are assuming that the smaller side is the primary side and that the join is Pk-Fk join.
                return 1.0 / card1;
            }
            return 1.0 / card2;
        }
    }

    // The expression we get may not be a base condition. It could be comprised of ors and ands and nots. So have to
    //recursively find the overall selectivity.
    protected double getSelectivityFromAnnotation(AbstractFunctionCallExpression afcExpr, boolean join)
            throws AlgebricksException {
        double sel = 1.0;

        if (afcExpr.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.OR)) {
            double orSel = getSelectivityFromAnnotation(
                    (AbstractFunctionCallExpression) afcExpr.getArguments().get(0).getValue(), join);
            for (int i = 1; i < afcExpr.getArguments().size(); i++) {
                ILogicalExpression lexpr = afcExpr.getArguments().get(i).getValue();
                if (lexpr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
                    sel = getSelectivityFromAnnotation(
                            (AbstractFunctionCallExpression) afcExpr.getArguments().get(i).getValue(), join);
                    orSel = orSel + sel - orSel * sel;
                }
            }
            return orSel;
        } else if (afcExpr.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.AND)) {
            double andSel = 1.0;
            for (int i = 0; i < afcExpr.getArguments().size(); i++) {
                ILogicalExpression lexpr = afcExpr.getArguments().get(i).getValue();
                if (lexpr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
                    sel = getSelectivityFromAnnotation(
                            (AbstractFunctionCallExpression) afcExpr.getArguments().get(i).getValue(), join);
                    andSel *= sel;
                }
            }
            return andSel;
        } else if (afcExpr.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.NOT)) {
            ILogicalExpression lexpr = afcExpr.getArguments().get(0).getValue();
            if (lexpr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
                sel = getSelectivityFromAnnotation(
                        (AbstractFunctionCallExpression) afcExpr.getArguments().get(0).getValue(), join);
                return 1.0 - sel;
            }
        }

        double s = 1.0;
        PredicateCardinalityAnnotation pca = afcExpr.getAnnotation(PredicateCardinalityAnnotation.class);
        if (pca != null) {
            s = pca.getSelectivity();
            sel *= s;
        } else {
            JoinProductivityAnnotation jpa = afcExpr.getAnnotation(JoinProductivityAnnotation.class);
            s = findJoinSelectivity(jpa, afcExpr);
            sel *= s;
        }
        if (join && s == 1.0) {
            // assume no selectivity was assigned
            joinEnum.singleDatasetPreds.add(afcExpr);
        }
        return sel;
    }

    public double getSelectivityFromAnnotationMain(ILogicalExpression leExpr, boolean join) throws AlgebricksException {
        double sel = 1.0;

        if (leExpr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
            AbstractFunctionCallExpression afcExpr = (AbstractFunctionCallExpression) leExpr;
            sel = getSelectivityFromAnnotation(afcExpr, join);
        }

        return sel;
    }

    // The next two routines should be combined and made more general
    protected double getSelectivity(ILogicalOperator op, boolean join) throws AlgebricksException {
        double sel = 1.0; // safe to return 1 if there is no annotation

        if (op == null) {
            return sel;
        }

        // find all the selectOperators here.
        while (op.getOperatorTag() != LogicalOperatorTag.EMPTYTUPLESOURCE) {
            if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
                SelectOperator selOper = (SelectOperator) op;
                sel *= getSelectivityFromAnnotationMain(selOper.getCondition().getValue(), join);
            }
            if (op.getOperatorTag() == LogicalOperatorTag.SUBPLAN) {
                sel *= getSelectivity((SubplanOperator) op);
            }
            op = op.getInputs().get(0).getValue();
        }
        return sel;
    }

    protected double getSelectivity(SubplanOperator subplanOp) throws AlgebricksException {
        double sel = 1.0; // safe to return 1 if there is no annotation
        //ILogicalOperator op = subplanOp;
        ILogicalOperator op = subplanOp.getNestedPlans().get(0).getRoots().get(0).getValue();
        while (true) {
            if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
                SelectOperator selOper = (SelectOperator) op;
                sel *= getSelectivityFromAnnotationMain(selOper.getCondition().getValue(), false);
            }
            if (op.getInputs().size() > 0) {
                op = op.getInputs().get(0).getValue();
            } else {
                break;
            }
        }
        return sel;
    }
}
