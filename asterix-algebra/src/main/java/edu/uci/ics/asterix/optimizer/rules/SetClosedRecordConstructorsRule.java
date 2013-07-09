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

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.typecomputer.base.TypeComputerUtilities;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.AbstractConstVarFunVisitor;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * open-record-constructor() becomes closed-record-constructor() if all the
 * branches below lead to dataset scans for closed record types
 */

public class SetClosedRecordConstructorsRule implements IAlgebraicRewriteRule {

    private SettingClosedRecordVisitor recordVisitor;

    public SetClosedRecordConstructorsRule() {
        this.recordVisitor = new SettingClosedRecordVisitor();
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext ctx) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (ctx.checkIfInDontApplySet(this, op)) {
            return false;
        }
        ctx.addToDontApplySet(this, op);
        this.recordVisitor.setOptimizationContext(ctx, op.computeInputTypeEnvironment(ctx));
        boolean res = op.acceptExpressionTransform(recordVisitor);
        if (res) {
            ctx.computeAndSetTypeEnvironmentForOperator(op);
        }
        return res;
    }

    private static class SettingClosedRecordVisitor extends AbstractConstVarFunVisitor<ClosedDataInfo, Void> implements
            ILogicalExpressionReferenceTransform {

        private IOptimizationContext context;
        private IVariableTypeEnvironment env;

        public void setOptimizationContext(IOptimizationContext context, IVariableTypeEnvironment env) {
            this.context = context;
            this.env = env;
        }

        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            AbstractLogicalExpression expr = (AbstractLogicalExpression) exprRef.getValue();
            ClosedDataInfo cdi = expr.accept(this, null);
            if (cdi.expressionChanged) {
                exprRef.setValue(cdi.expression);
            }
            return cdi.expressionChanged;
        }

        @Override
        public ClosedDataInfo visitConstantExpression(ConstantExpression expr, Void arg) throws AlgebricksException {
            return new ClosedDataInfo(false, hasClosedType(expr), expr);
        }

        @Override
        public ClosedDataInfo visitFunctionCallExpression(AbstractFunctionCallExpression expr, Void arg)
                throws AlgebricksException {
            boolean allClosed = true;
            boolean changed = false;
            if (expr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.OPEN_RECORD_CONSTRUCTOR)) {
                ARecordType reqType = (ARecordType) TypeComputerUtilities.getRequiredType(expr);
                if (reqType == null || !reqType.isOpen()) {
                    int n = expr.getArguments().size();
                    if (n % 2 > 0) {
                        throw new AlgebricksException(
                                "Record constructor expected to have an even number of arguments: " + expr);
                    }
                    for (int i = 0; i < n / 2; i++) {
                        ILogicalExpression a0 = expr.getArguments().get(2 * i).getValue();
                        if (a0.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                            allClosed = false;
                        }
                        Mutable<ILogicalExpression> aRef1 = expr.getArguments().get(2 * i + 1);
                        ILogicalExpression a1 = aRef1.getValue();
                        ClosedDataInfo cdi = a1.accept(this, arg);
                        if (!cdi.dataIsClosed) {
                            allClosed = false;
                        }
                        if (cdi.expressionChanged) {
                            aRef1.setValue(cdi.expression);
                            changed = true;
                        }
                    }
                    if (allClosed) {
                        expr.setFunctionInfo(FunctionUtils
                                .getFunctionInfo(AsterixBuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR));
                        GlobalConfig.ASTERIX_LOGGER.finest("Switching to CLOSED record constructor in " + expr + ".\n");
                        changed = true;
                    }
                }
            } else {
                boolean rewrite = true;
                if (expr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.ORDERED_LIST_CONSTRUCTOR)
                        || (expr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.UNORDERED_LIST_CONSTRUCTOR))) {
                    IAType reqType = TypeComputerUtilities.getRequiredType(expr);
                    if (reqType == null) {
                        rewrite = false;
                    }
                }
                if (rewrite) {
                    for (Mutable<ILogicalExpression> e : expr.getArguments()) {
                        ILogicalExpression ale = e.getValue();
                        ClosedDataInfo cdi = ale.accept(this, arg);
                        if (!cdi.dataIsClosed) {
                            allClosed = false;
                        }
                        if (cdi.expressionChanged) {
                            e.setValue(cdi.expression);
                            changed = true;
                        }
                    }
                }
            }
            return new ClosedDataInfo(changed, hasClosedType(expr), expr);
        }

        @Override
        public ClosedDataInfo visitVariableReferenceExpression(VariableReferenceExpression expr, Void arg)
                throws AlgebricksException {
            Object varType = env.getVarType(expr.getVariableReference());
            if (varType == null) {
                throw new AlgebricksException("Could not infer type for variable '" + expr.getVariableReference()
                        + "'.");
            }
            boolean dataIsClosed = isClosedRec((IAType) varType);
            return new ClosedDataInfo(false, dataIsClosed, expr);
        }

        private boolean hasClosedType(ILogicalExpression expr) throws AlgebricksException {
            IAType t = (IAType) context.getExpressionTypeComputer().getType(expr, context.getMetadataProvider(), env);
            return isClosedRec(t);
        }

        private static boolean isClosedRec(IAType t) throws AlgebricksException {
            switch (t.getTypeTag()) {
                case ANY: {
                    return false;
                }
                case CIRCLE:
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case BINARY:
                case BITARRAY:
                case FLOAT:
                case DOUBLE:
                case STRING:
                case LINE:
                case NULL:
                case BOOLEAN:
                case DATETIME:
                case DATE:
                case TIME:
                case DURATION:
                case YEARMONTHDURATION:
                case DAYTIMEDURATION:
                case INTERVAL:
                case POINT:
                case POINT3D:
                case POLYGON:
                case RECTANGLE: {
                    return true;
                }
                case RECORD: {
                    return !((ARecordType) t).isOpen();
                }
                case UNION: {
                    AUnionType ut = (AUnionType) t;
                    for (IAType t2 : ut.getUnionList()) {
                        if (!isClosedRec(t2)) {
                            return false;
                        }
                    }
                    return true;
                }
                case ORDEREDLIST: {
                    AOrderedListType ol = (AOrderedListType) t;
                    return isClosedRec(ol.getItemType());
                }
                case UNORDEREDLIST: {
                    AUnorderedListType ul = (AUnorderedListType) t;
                    return isClosedRec(ul.getItemType());
                }
                default: {
                    throw new IllegalStateException("Closed type analysis not implemented for type " + t);
                }
            }
        }
    }

    private static class ClosedDataInfo {
        boolean expressionChanged;
        boolean dataIsClosed;
        ILogicalExpression expression;

        public ClosedDataInfo(boolean expressionChanged, boolean dataIsClosed, ILogicalExpression expression) {
            this.expressionChanged = expressionChanged;
            this.dataIsClosed = dataIsClosed;
            this.expression = expression;
        }
    }
}
