package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.metadata.declared.AqlDataSource;
import edu.uci.ics.asterix.om.typecomputer.base.TypeComputerUtilities;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class TopDownTypeInferenceRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        /**
         * pattern match: sink/insert/assign
         * record type is propagated from insert data source to the record-constructor expression
         */
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (op1.getOperatorTag() != LogicalOperatorTag.SINK)
            return false;
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) op1.getInputs().get(0).getValue();
        if (op2.getOperatorTag() != LogicalOperatorTag.INSERT_DELETE)
            return false;
        AbstractLogicalOperator op3 = (AbstractLogicalOperator) op2.getInputs().get(0).getValue();
        if (op3.getOperatorTag() != LogicalOperatorTag.ASSIGN)
            return false;

        /**
         * get required record type
         */
        InsertDeleteOperator insertDeleteOperator = (InsertDeleteOperator) op2;
        AssignOperator oldAssignOperator = (AssignOperator) op3;
        AqlDataSource dataSource = (AqlDataSource) insertDeleteOperator.getDataSource();
        IAType[] schemaTypes = (IAType[]) dataSource.getSchemaTypes();
        ARecordType requiredRecordType = (ARecordType) schemaTypes[schemaTypes.length - 1];

        /**
         * get input record type to the insert operator
         */
        List<LogicalVariable> usedVariables = new ArrayList<LogicalVariable>();
        VariableUtilities.getUsedVariables(oldAssignOperator, usedVariables);
        if (usedVariables.size() == 0)
            return false;
        LogicalVariable oldRecordVariable = usedVariables.get(0);
        LogicalVariable inputRecordVar = usedVariables.get(0);
        IVariableTypeEnvironment env = oldAssignOperator.computeOutputTypeEnvironment(context);
        ARecordType inputRecordType = (ARecordType) env.getVarType(inputRecordVar);

        AbstractLogicalOperator currentOperator = oldAssignOperator;
        List<LogicalVariable> producedVariables = new ArrayList<LogicalVariable>();
        boolean changed = false;
        if (!requiredRecordType.equals(inputRecordType)) {
            /**
             * find the assign operator for the "input record" to the
             * insert_delete operator
             */
            do {
                if (currentOperator.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                    producedVariables.clear();
                    VariableUtilities.getProducedVariables(currentOperator, producedVariables);
                    int position = producedVariables.indexOf(oldRecordVariable);

                    /**
                     * set the top-down propagated type
                     */
                    if (position >= 0) {
                        AssignOperator originalAssign = (AssignOperator) currentOperator;
                        List<Mutable<ILogicalExpression>> expressionPointers = originalAssign.getExpressions();
                        ILogicalExpression expr = expressionPointers.get(position).getValue();
                        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                            ScalarFunctionCallExpression funcExpr = (ScalarFunctionCallExpression) expr;
                            changed = TypeComputerUtilities.setRequiredType(funcExpr, requiredRecordType);
                            List<Mutable<ILogicalExpression>> args = funcExpr.getArguments();
                            int openPartStart = requiredRecordType.getFieldTypes().length * 2;
                            for (int j = openPartStart; j < args.size(); j++) {
                                ILogicalExpression arg = args.get(j).getValue();
                                if (arg.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                                    AbstractFunctionCallExpression argFunc = (AbstractFunctionCallExpression) arg;
                                    TypeComputerUtilities.setOpenType(argFunc, true);
                                }
                            }
                        }
                    }
                }
                if (currentOperator.getInputs().size() > 0)
                    currentOperator = (AbstractLogicalOperator) currentOperator.getInputs().get(0).getValue();
                else
                    break;
            } while (currentOperator != null);
        }
        return changed;
    }
}
