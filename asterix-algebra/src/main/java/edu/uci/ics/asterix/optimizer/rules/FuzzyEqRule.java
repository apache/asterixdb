package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.common.functions.FunctionUtils;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeHelper;
import edu.uci.ics.asterix.optimizer.base.FuzzyUtils;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class FuzzyEqRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        // current operator is INNERJOIN or LEFTOUTERJOIN or SELECT
        Mutable<ILogicalExpression> expRef;
        if (op.getOperatorTag() == LogicalOperatorTag.INNERJOIN
                || op.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN) {
            AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) op;
            expRef = joinOp.getCondition();
        } else if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
            SelectOperator selectOp = (SelectOperator) op;
            expRef = selectOp.getCondition();
        } else {
            return false;
        }

        AqlCompiledMetadataDeclarations aqlMetadata = ((AqlMetadataProvider) context.getMetadataProvider())
                .getMetadataDeclarations();

        IVariableTypeEnvironment env = context.getOutputTypeEnvironment(op);
        if (expandFuzzyEq(expRef, context, env, aqlMetadata)) {
            context.computeAndSetTypeEnvironmentForOperator(op);
            return true;
        }
        return false;
    }

    private boolean expandFuzzyEq(Mutable<ILogicalExpression> expRef, IOptimizationContext context,
            IVariableTypeEnvironment env, AqlCompiledMetadataDeclarations aqlMetadata) throws AlgebricksException {
        ILogicalExpression exp = expRef.getValue();

        if (exp.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }

        boolean expanded = false;
        AbstractFunctionCallExpression funcExp = (AbstractFunctionCallExpression) exp;
        FunctionIdentifier fi = funcExp.getFunctionIdentifier();

        if (fi == AsterixBuiltinFunctions.FUZZY_EQ) {
            List<Mutable<ILogicalExpression>> inputExps = funcExp.getArguments();

            ArrayList<Mutable<ILogicalExpression>> similarityArgs = new ArrayList<Mutable<ILogicalExpression>>();
            for (int i = 0; i < 2; i++) {
                Mutable<ILogicalExpression> inputExpRef = inputExps.get(i);
                ILogicalExpression inputExp = inputExpRef.getValue();

                // get the input type tag
                ATypeTag inputTypeTag = null;
                if (inputExp.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                    VariableReferenceExpression inputVarRef = (VariableReferenceExpression) inputExp;
                    LogicalVariable inputVar = inputVarRef.getVariableReference();
                    IAType t = TypeHelper.getNonOptionalType((IAType) env.getVarType(inputVar));
                    inputTypeTag = t.getTypeTag();
                } else if (inputExp.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    // TODO
                    // AbstractFunctionCallExpression inputFuncCall =
                    // (AbstractFunctionCallExpression) inputExp;
                    throw new NotImplementedException();
                } else if (inputExp.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                    // TODO
                    // ConstantExpression inputConst = (ConstantExpression)
                    // inputExp;
                    throw new NotImplementedException();
                } else {
                    throw new NotImplementedException();
                }

                // get the tokenizer (if any)
                FunctionIdentifier tokenizer = FuzzyUtils.getTokenizer(inputTypeTag);
                if (tokenizer == null) {
                    similarityArgs.add(inputExpRef);
                } else {
                    ArrayList<Mutable<ILogicalExpression>> tokenizerArguments = new ArrayList<Mutable<ILogicalExpression>>();
                    tokenizerArguments.add(inputExpRef);
                    ScalarFunctionCallExpression tokenizerExp = new ScalarFunctionCallExpression(
                            FunctionUtils.getFunctionInfo(tokenizer), tokenizerArguments);
                    similarityArgs.add(new MutableObject<ILogicalExpression>(tokenizerExp));
                }
            }

            // TODO use similarity-*-check

            // similarityArgs.add(new Mutable<ILogicalExpression>(new
            // ConstantExpression(new FloatLiteral(FuzzyUtils
            // .getSimThreshold(aqlMetadata)))));

            String simFunctionName = FuzzyUtils.getSimFunction(aqlMetadata);

            // FunctionIdentifier simFunctionIdentifier = new
            // FunctionIdentifier(AsterixBuiltinFunctions.ASTERIX_NS,
            // "similarity-" + simFunctionName + "-check");

            FunctionIdentifier simFunctionIdentifier = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
                    "similarity-" + simFunctionName, true);

            ScalarFunctionCallExpression similarityExp = new ScalarFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(simFunctionIdentifier), similarityArgs);

            // ArrayList<Mutable<ILogicalExpression>> atArgs = new
            // ArrayList<Mutable<ILogicalExpression>>();
            // atArgs.add(new Mutable<ILogicalExpression>(similarityExp));
            // atArgs.add(new Mutable<ILogicalExpression>(new
            // ConstantExpression(new IntegerLiteral(0))));

            // ScalarFunctionCallExpression atExp = new
            // ScalarFunctionCallExpression(
            // FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.GET_ITEM),
            // atArgs);

            // expRef.setValue(atExp);

            ArrayList<Mutable<ILogicalExpression>> geArgs = new ArrayList<Mutable<ILogicalExpression>>();
            geArgs.add(new MutableObject<ILogicalExpression>(similarityExp));
            float f = FuzzyUtils.getSimThreshold(aqlMetadata);
            geArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AFloat(f)))));

            ScalarFunctionCallExpression geExp = new ScalarFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(AlgebricksBuiltinFunctions.GE), geArgs);

            expRef.setValue(geExp);

            return true;

        } else if (fi == AlgebricksBuiltinFunctions.AND || fi == AlgebricksBuiltinFunctions.OR) {
            for (int i = 0; i < 2; i++) {
                if (expandFuzzyEq(funcExp.getArguments().get(i), context, env, aqlMetadata)) {
                    expanded = true;
                }
            }
        }

        return expanded;
    }

    @Override
    public boolean rewritePre( Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }
}
