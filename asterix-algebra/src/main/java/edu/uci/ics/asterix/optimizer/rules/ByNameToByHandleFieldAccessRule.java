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
package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.algebra.base.AsterixOperatorAnnotations;
import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ByNameToByHandleFieldAccessRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assign = (AssignOperator) op;
        if (assign.getAnnotations().get(AsterixOperatorAnnotations.PUSHED_FIELD_ACCESS) == null) {
            return false;
        }
        byNameToByHandle(assign, context);
        return true;
    }

    private static void byNameToByHandle(AssignOperator fieldAccessOp, IOptimizationContext context) {
        Mutable<ILogicalOperator> opUnder = fieldAccessOp.getInputs().get(0);
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) fieldAccessOp.getExpressions().get(0)
                .getValue();
        ILogicalExpression a1 = fce.getArguments().get(0).getValue();

        VariableReferenceExpression x;
        if (a1.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            x = (VariableReferenceExpression) a1;
        } else {
            LogicalVariable var1 = context.newVar();
            ArrayList<LogicalVariable> varArray = new ArrayList<LogicalVariable>(1);
            varArray.add(var1);
            ArrayList<Mutable<ILogicalExpression>> exprArray = new ArrayList<Mutable<ILogicalExpression>>(1);
            exprArray.add(new MutableObject<ILogicalExpression>(a1));
            AssignOperator assignVar = new AssignOperator(varArray, exprArray);
            x = new VariableReferenceExpression(var1);
            assignVar.getInputs().add(opUnder);
            opUnder = new  MutableObject<ILogicalOperator>(assignVar);
        }

        // let $t := type-of(x)
        LogicalVariable t = context.newVar();

        AbstractFunctionCallExpression typeOf = new ScalarFunctionCallExpression(
                FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.TYPE_OF));
        typeOf.getArguments().add(new MutableObject<ILogicalExpression>(x));
        AssignOperator typAssign = new AssignOperator(t, new MutableObject<ILogicalExpression>(typeOf));
        typAssign.getInputs().add(opUnder);

        // let $w := get-handle($t, path-expression)
        LogicalVariable w = context.newVar();
        AbstractFunctionCallExpression getHandle = new ScalarFunctionCallExpression(
                FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.GET_HANDLE));
        getHandle.getArguments().add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(t)));
        // the accessed field
        getHandle.getArguments().add(new MutableObject<ILogicalExpression>(fce.getArguments().get(1).getValue()));
        AssignOperator handleAssign = new AssignOperator(w, new MutableObject<ILogicalExpression>(getHandle));
        handleAssign.getInputs().add(new MutableObject<ILogicalOperator>(typAssign));

        // let $y := get-data(x, $w)
        AbstractFunctionCallExpression getData = new ScalarFunctionCallExpression(
                FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.GET_DATA));
        VariableReferenceExpression ref2 = new VariableReferenceExpression(x.getVariableReference());
        getData.getArguments().add(new MutableObject<ILogicalExpression>(ref2));
        getData.getArguments().add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(w)));
        fieldAccessOp.getExpressions().get(0).setValue(getData);
        List<Mutable<ILogicalOperator>> faInputs = fieldAccessOp.getInputs();
        faInputs.clear();
        faInputs.add(new MutableObject<ILogicalOperator>(handleAssign));

        // fieldAccess.setAnnotation(OperatorAnnotation.FIELD_ACCESS,
        // fce.getArguments().get(0));
        fieldAccessOp.removeAnnotation(AsterixOperatorAnnotations.PUSHED_FIELD_ACCESS);
    }

}
