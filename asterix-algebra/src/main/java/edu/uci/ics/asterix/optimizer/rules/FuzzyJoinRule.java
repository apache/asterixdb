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

import java.io.StringReader;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.algebra.base.LogicalOperatorDeepCopyVisitor;
import edu.uci.ics.asterix.aql.base.Clause;
import edu.uci.ics.asterix.aql.expression.Identifier;
import edu.uci.ics.asterix.aqlplus.parser.AQLPlusParser;
import edu.uci.ics.asterix.aqlplus.parser.ParseException;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeHelper;
import edu.uci.ics.asterix.optimizer.base.FuzzyUtils;
import edu.uci.ics.asterix.translator.AqlPlusExpressionToPlanTranslator;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.Counter;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IndexedNLJoinExpressionAnnotation;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class FuzzyJoinRule implements IAlgebraicRewriteRule {

    private static HashSet<FunctionIdentifier> simFuncs = new HashSet<FunctionIdentifier>();
    static {
        simFuncs.add(AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK);
    }

    private static final String AQLPLUS = ""
            //
            // -- - Stage 3 - --
            //
            + "((#RIGHT), "
            + "  (join((#LEFT), "
            //
            // -- -- - Stage 2 - --
            //
            + "    ("
            + "    join( "
            + "      ( "
            + "      #LEFT_1 "
            + "      let $tokensUnrankedLeft := %s($$LEFT_1) "
            + "      let $lenLeft := len($tokensUnrankedLeft) "
            + "      let $tokensLeft := "
            + "        for $token in $tokensUnrankedLeft "
            + "        for $tokenRanked at $i in "
            //
            // -- -- -- - Stage 1 - --
            //
            // + "          #LEFT_2 "
            // + "          let $id := $$LEFTPK_2 "
            // + "          for $token in %s($$LEFT_2) "
            + "          #RIGHT_2 "
            + "          let $id := $$RIGHTPK_2 "
            + "          for $token in %s($$RIGHT_2) "
            + "          /*+ hash */ "
            + "          group by $tokenGroupped := $token with $id "
            + "          /*+ inmem 34 198608 */ "
            + "          order by count($id), $tokenGroupped "
            + "          return $tokenGroupped "
            //
            // -- -- -- -
            //
            + "        where $token = /*+ bcast */ $tokenRanked "
            + "        order by $i "
            + "        return $i "
            + "      for $prefixTokenLeft in subset-collection($tokensLeft, 0, prefix-len-%s(len($tokensLeft), %ff)) "
            + "      ),( "
            + "      #RIGHT_1 "
            + "      let $tokensUnrankedRight := %s($$RIGHT_1) "
            + "      let $lenRight := len($tokensUnrankedRight) "
            + "      let $tokensRight := "
            + "        for $token in $tokensUnrankedRight "
            + "        for $tokenRanked at $i in "
            //
            // -- -- -- - Stage 1 - --
            //
            // + "          #LEFT_3 "
            // + "          let $id := $$LEFTPK_3 "
            // + "          for $token in %s($$LEFT_3) "
            + "          #RIGHT_3 "
            + "          let $id := $$RIGHTPK_3 "
            + "          for $token in %s($$RIGHT_3) "
            + "          /*+ hash */ "
            + "          group by $tokenGroupped := $token with $id "
            + "          /*+ inmem 34 198608 */ "
            + "          order by count($id), $tokenGroupped "
            + "          return $tokenGroupped "
            //
            // -- -- -- -
            //
            + "        where $token = /*+ bcast */ $tokenRanked "
            + "        order by $i "
            + "        return $i "
            + "      for $prefixTokenRight in subset-collection($tokensRight, 0, prefix-len-%s(len($tokensRight), %ff)) "
            + "      ), $prefixTokenLeft = $prefixTokenRight) "
            + "    let $sim := similarity-%s-prefix($lenLeft, $tokensLeft, $lenRight, $tokensRight, $prefixTokenLeft, %ff) "
            + "    where $sim >= %ff " + "    /*+ hash*/ "
            + "    group by $idLeft := $$LEFTPK_1, $idRight := $$RIGHTPK_1 with $sim "
            //
            // -- -- -
            //
            + "    ), $$LEFTPK = $idLeft)),  $$RIGHTPK = $idRight)";

    private Collection<LogicalVariable> liveVars = new HashSet<LogicalVariable>();

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        // current opperator is join
        if (op.getOperatorTag() != LogicalOperatorTag.INNERJOIN
                && op.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }

        // Find GET_ITEM function.
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) op;
        Mutable<ILogicalExpression> expRef = joinOp.getCondition();
        Mutable<ILogicalExpression> getItemExprRef = getSimilarityExpression(expRef);
        if (getItemExprRef == null) {
            return false;
        }
        // Check if the GET_ITEM function is on one of the supported similarity-check functions.
        AbstractFunctionCallExpression getItemFuncExpr = (AbstractFunctionCallExpression) getItemExprRef.getValue();
        Mutable<ILogicalExpression> argRef = getItemFuncExpr.getArguments().get(0);
        AbstractFunctionCallExpression simFuncExpr = (AbstractFunctionCallExpression) argRef.getValue();
        if (!simFuncs.contains(simFuncExpr.getFunctionIdentifier())) {
            return false;
        }
        // Skip this rule based on annotations.
        if (simFuncExpr.getAnnotations().containsKey(IndexedNLJoinExpressionAnnotation.INSTANCE)) {
            return false;
        }

        List<Mutable<ILogicalOperator>> inputOps = joinOp.getInputs();
        ILogicalOperator leftInputOp = inputOps.get(0).getValue();
        ILogicalOperator rightInputOp = inputOps.get(1).getValue();

        List<Mutable<ILogicalExpression>> inputExps = simFuncExpr.getArguments();

        ILogicalExpression inputExp0 = inputExps.get(0).getValue();
        ILogicalExpression inputExp1 = inputExps.get(1).getValue();

        // left and right expressions are variables
        if (inputExp0.getExpressionTag() != LogicalExpressionTag.VARIABLE
                || inputExp1.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }

        LogicalVariable inputVar0 = ((VariableReferenceExpression) inputExp0).getVariableReference();
        LogicalVariable inputVar1 = ((VariableReferenceExpression) inputExp1).getVariableReference();

        LogicalVariable leftInputVar;
        LogicalVariable rightInputVar;

        liveVars.clear();
        VariableUtilities.getLiveVariables(leftInputOp, liveVars);
        if (liveVars.contains(inputVar0)) {
            leftInputVar = inputVar0;
            rightInputVar = inputVar1;
        } else {
            leftInputVar = inputVar1;
            rightInputVar = inputVar0;
        }

        List<LogicalVariable> leftInputPKs = context.findPrimaryKey(leftInputVar);
        List<LogicalVariable> rightInputPKs = context.findPrimaryKey(rightInputVar);
        // Bail if primary keys could not be inferred.
        if (leftInputPKs == null || rightInputPKs == null) {
            return false;
        }
        // primary key has only one variable
        if (leftInputPKs.size() != 1 || rightInputPKs.size() != 1) {
            return false;
        }
        IAType leftType = (IAType) context.getOutputTypeEnvironment(leftInputOp).getVarType(leftInputVar);
        IAType rightType = (IAType) context.getOutputTypeEnvironment(rightInputOp).getVarType(rightInputVar);
        // left-hand side and right-hand side of "~=" has the same type
        IAType left2 = TypeHelper.getNonOptionalType(leftType);
        IAType right2 = TypeHelper.getNonOptionalType(rightType);
        if (!left2.deepEqual(right2)) {
            return false;
        }
        //
        // -- - FIRE - --
        //
        AqlMetadataProvider metadataProvider = ((AqlMetadataProvider) context.getMetadataProvider());
        FunctionIdentifier funcId = FuzzyUtils.getTokenizer(leftType.getTypeTag());
        String tokenizer;
        if (funcId == null) {
            tokenizer = "";
        } else {
            tokenizer = funcId.getName();
        }

        float simThreshold = FuzzyUtils.getSimThreshold(metadataProvider);
        String simFunction = FuzzyUtils.getSimFunction(metadataProvider);

        // finalize AQL+ query
        String prepareJoin;
        switch (joinOp.getJoinKind()) {
            case INNER: {
                prepareJoin = "join" + AQLPLUS;
                break;
            }
            case LEFT_OUTER: {
                // TODO To make it work for Left Outer Joins, we should permute
                // the #LEFT and #RIGHT at the top of the AQL+ query. But, when
                // doing this, the
                // fuzzyjoin/user-vis-int-vis-user-lot-aqlplus_1.aql (the one
                // doing 3-way fuzzy joins) gives a different result. But even
                // if we don't change the FuzzyJoinRule, permuting the for
                // clauses in fuzzyjoin/user-vis-int-vis-user-lot-aqlplus_1.aql
                // leads to different results, which suggests there is some
                // other sort of bug.
                return false;
                // prepareJoin = "loj" + AQLPLUS;
                // break;
            }
            default: {
                throw new IllegalStateException();
            }
        }
        String aqlPlus = String.format(Locale.US, prepareJoin, tokenizer, tokenizer, simFunction, simThreshold,
                tokenizer, tokenizer, simFunction, simThreshold, simFunction, simThreshold, simThreshold);

        LogicalVariable leftPKVar = leftInputPKs.get(0);
        LogicalVariable rightPKVar = rightInputPKs.get(0);

        Counter counter = new Counter(context.getVarCounter());

        AQLPlusParser parser = new AQLPlusParser(new StringReader(aqlPlus));
        parser.initScope();
        parser.setVarCounter(counter);
        List<Clause> clauses;
        try {
            clauses = parser.Clauses();
        } catch (ParseException e) {
            throw new AlgebricksException(e);
        }
        // The translator will compile metadata internally. Run this compilation
        // under the same transaction id as the "outer" compilation.
        AqlPlusExpressionToPlanTranslator translator = new AqlPlusExpressionToPlanTranslator(
                metadataProvider.getJobId(), metadataProvider, counter, null, null);

        LogicalOperatorDeepCopyVisitor deepCopyVisitor = new LogicalOperatorDeepCopyVisitor(counter);

        translator.addOperatorToMetaScope(new Identifier("#LEFT"), leftInputOp);
        translator.addVariableToMetaScope(new Identifier("$$LEFT"), leftInputVar);
        translator.addVariableToMetaScope(new Identifier("$$LEFTPK"), leftPKVar);

        translator.addOperatorToMetaScope(new Identifier("#RIGHT"), rightInputOp);
        translator.addVariableToMetaScope(new Identifier("$$RIGHT"), rightInputVar);
        translator.addVariableToMetaScope(new Identifier("$$RIGHTPK"), rightPKVar);

        translator.addOperatorToMetaScope(new Identifier("#LEFT_1"), deepCopyVisitor.deepCopy(leftInputOp, null));
        translator.addVariableToMetaScope(new Identifier("$$LEFT_1"), deepCopyVisitor.varCopy(leftInputVar));
        translator.addVariableToMetaScope(new Identifier("$$LEFTPK_1"), deepCopyVisitor.varCopy(leftPKVar));
        deepCopyVisitor.updatePrimaryKeys(context);
        deepCopyVisitor.reset();

        // translator.addOperatorToMetaScope(new Identifier("#LEFT_2"),
        // deepCopyVisitor.deepCopy(leftInputOp, null));
        // translator.addVariableToMetaScope(new Identifier("$$LEFT_2"),
        // deepCopyVisitor.varCopy(leftInputVar));
        // translator.addVariableToMetaScope(new Identifier("$$LEFTPK_2"),
        // deepCopyVisitor.varCopy(leftPKVar));
        // deepCopyVisitor.updatePrimaryKeys(context);
        // deepCopyVisitor.reset();
        //
        // translator.addOperatorToMetaScope(new Identifier("#LEFT_3"),
        // deepCopyVisitor.deepCopy(leftInputOp, null));
        // translator.addVariableToMetaScope(new Identifier("$$LEFT_3"),
        // deepCopyVisitor.varCopy(leftInputVar));
        // translator.addVariableToMetaScope(new Identifier("$$LEFTPK_3"),
        // deepCopyVisitor.varCopy(leftPKVar));
        // deepCopyVisitor.updatePrimaryKeys(context);
        // deepCopyVisitor.reset();

        translator.addOperatorToMetaScope(new Identifier("#RIGHT_1"), deepCopyVisitor.deepCopy(rightInputOp, null));
        translator.addVariableToMetaScope(new Identifier("$$RIGHT_1"), deepCopyVisitor.varCopy(rightInputVar));
        translator.addVariableToMetaScope(new Identifier("$$RIGHTPK_1"), deepCopyVisitor.varCopy(rightPKVar));
        deepCopyVisitor.updatePrimaryKeys(context);
        deepCopyVisitor.reset();

        // TODO pick side to run Stage 1, currently always picks RIGHT side
        translator.addOperatorToMetaScope(new Identifier("#RIGHT_2"), deepCopyVisitor.deepCopy(rightInputOp, null));
        translator.addVariableToMetaScope(new Identifier("$$RIGHT_2"), deepCopyVisitor.varCopy(rightInputVar));
        translator.addVariableToMetaScope(new Identifier("$$RIGHTPK_2"), deepCopyVisitor.varCopy(rightPKVar));
        deepCopyVisitor.updatePrimaryKeys(context);
        deepCopyVisitor.reset();

        translator.addOperatorToMetaScope(new Identifier("#RIGHT_3"), deepCopyVisitor.deepCopy(rightInputOp, null));
        translator.addVariableToMetaScope(new Identifier("$$RIGHT_3"), deepCopyVisitor.varCopy(rightInputVar));
        translator.addVariableToMetaScope(new Identifier("$$RIGHTPK_3"), deepCopyVisitor.varCopy(rightPKVar));
        deepCopyVisitor.updatePrimaryKeys(context);
        deepCopyVisitor.reset();

        ILogicalPlan plan;
        try {
            plan = translator.translate(clauses);
        } catch (AsterixException e) {
            throw new AlgebricksException(e);
        }
        context.setVarCounter(counter.get());

        ILogicalOperator outputOp = plan.getRoots().get(0).getValue();

        SelectOperator extraSelect = null;
        if (getItemExprRef != expRef) {
            // more than one join condition
            getItemExprRef.setValue(ConstantExpression.TRUE);
            switch (joinOp.getJoinKind()) {
                case INNER: {
                    extraSelect = new SelectOperator(expRef);
                    extraSelect.getInputs().add(new MutableObject<ILogicalOperator>(outputOp));
                    outputOp = extraSelect;
                    break;
                }
                case LEFT_OUTER: {
                    if (((AbstractLogicalOperator) outputOp).getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
                        throw new IllegalStateException();
                    }
                    LeftOuterJoinOperator topJoin = (LeftOuterJoinOperator) outputOp;
                    topJoin.getCondition().setValue(expRef.getValue());
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }
        }
        opRef.setValue(outputOp);
        OperatorPropertiesUtil.typeOpRec(opRef, context);
        return true;
    }

    /**
     * Look for GET_ITEM function call.
     */
    private Mutable<ILogicalExpression> getSimilarityExpression(Mutable<ILogicalExpression> expRef) {
        ILogicalExpression exp = expRef.getValue();
        if (exp.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) exp;
            if (funcExpr.getFunctionIdentifier().equals(AsterixBuiltinFunctions.GET_ITEM)) {
                return expRef;
            }
            if (funcExpr.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.AND)) {
                for (int i = 0; i < 2; i++) {
                    Mutable<ILogicalExpression> expRefRet = getSimilarityExpression(funcExpr.getArguments().get(i));
                    if (expRefRet != null) {
                        return expRefRet;
                    }
                }
            }
        }
        return null;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }
}
