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
package org.apache.asterix.optimizer.rules;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

import org.apache.asterix.aqlplus.parser.AQLPlusParser;
import org.apache.asterix.aqlplus.parser.ParseException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.base.FuzzyUtils;
import org.apache.asterix.translator.AqlPlusExpressionToPlanTranslator;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.Counter;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IndexedNLJoinExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.IsomorphismUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.LogicalOperatorDeepCopyWithNewVariablesVisitor;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class FuzzyJoinRule implements IAlgebraicRewriteRule {

    private static HashSet<FunctionIdentifier> simFuncs = new HashSet<>();

    static {
        simFuncs.add(BuiltinFunctions.SIMILARITY_JACCARD_CHECK);
    }

    private final List<Collection<LogicalVariable>> previousPKs = new ArrayList<>();

    // Please correspond the value to the anchors $$RIGHT_##_0 and ##RIGHT_3 in AQLPLUS.
    private static final int SUBSET_RIGHT_INDEX = 1; // Corresponds to $$RIGHT_1_0 and ##RIGHT_1
    // private static final int LENGTH_LEFT_INDEX = 2;  // Corresponds to $$RIGHT_2_0 and ##RIGHT_2
    private static final int LENGTH_RIGHT_INDEX = 3; // Corresponds to $$RIGHT_3_0 and ##RIGHT_3

    // Step1: Initialize the host embedding aql to substitute the fuzzy equal condition such as $r.a ~= $s.b
    private static final String AQLPLUS = ""
            //
            // -- - Stage 3 - --
            //
            + "((##LEFT_0), " + "  (join((##RIGHT_0), "
            //
            // -- -- - Stage 2 - --
            //
            + "    (" + "join( " + "( " + "##RIGHT_1 " + "      let $tokensUnrankedRight := %s($$RIGHT_1) "
            + "    let $lenRight := len($tokensUnrankedRight) " + "      let $tokensRight := "
            + "    for $token in $tokensUnrankedRight " + "for $tokenRanked at $i in "
            //
            // -- -- -- - Stage 1 - --
            // Since we use the right join branch R to generate our token order, this can shorten the prefix length of S
            // in case of R ~= S, where some tokens are in S but not in R.
            + "          ##RIGHT_3 " + "let $id := $$RIGHTPK_3_0 " + "for $token in %s($$RIGHT_3) "
            + "          /*+ hash */ " + "group by $tokenGroupped := $token with $id "
            + "          order by count($id), $tokenGroupped return $tokenGroupped "
            //
            // -- -- -- -
            //
            + "        where $token = /*+ bcast */ $tokenRanked " + "order by $i " + "return $i "
            + "      for $prefixTokenRight in subset-collection($tokensRight, 0, prefix-len-%s(len($tokensRight), %ff)) "
            + "      ), " + "( " + "##LEFT_1 " + "let $tokensUnrankedLeft := %s($$LEFT_1) "
            + "      let $lenLeft := len($tokensUnrankedLeft) " + "let $tokensLeft := "
            + "        for $token in $tokensUnrankedLeft " + "for $tokenRanked at $i in "
            //
            // -- -- -- - Stage 1 - --
            + "          ##RIGHT_2 " + "let $id := $$RIGHTPK_2_0 " + "for $token in %s($$RIGHT_2) "
            + "          /*+ hash */ " + "group by $tokenGroupped := $token with $id "
            + "          order by count($id), $tokenGroupped return $tokenGroupped "
            //
            // -- -- -- -
            //
            + "        where $token = /*+ bcast */ $tokenRanked " + "order by $i " + "return $i "
            // We use the input string $tokensUnrankedLeft instead of $tokensLeft to ensure it will not miss similar
            // pairs when the prefix of S has been reduced in case of R ~= S, where some tokens are in S but not in R.
            + "      let $actualPreLen := prefix-len-%s(len($tokensUnrankedLeft), %ff) - $lenLeft + len($tokensLeft) "
            + "      for $prefixTokenLeft in subset-collection($tokensLeft, 0, $actualPreLen)) "
            + "      , $prefixTokenLeft = $prefixTokenRight) "
            + "let $sim := similarity-%s-prefix($lenRight, $tokensRight, $lenLeft, $tokensLeft, $prefixTokenLeft, %ff) "
            + "where $sim >= %ff " + "/*+ hash*/ " + "group by %s, %s with $sim "
            //
            // -- -- -
            //
            + "    ), %s)),  %s)";

    private static final String GROUPBY_LEFT = "$idLeft_%d := $$LEFTPK_1_%d";
    private static final String GROUPBY_RIGHT = "$idRight_%d := $$RIGHTPK_1_%d";
    private static final String JOIN_COND_LEFT = "$$LEFTPK_0_%d = $idLeft_%d";
    private static final String JOIN_COND_RIGHT = "$$RIGHTPK_0_%d = $idRight_%d";
    private static final String AQLPLUS_INNER_JOIN = "join";
    private static final String AQLPLUS_LEFTOUTER_JOIN = "loj";

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        // current operator should be a join.
        if (op.getOperatorTag() != LogicalOperatorTag.INNERJOIN
                && op.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }

        // Finds GET_ITEM function in the join condition.
        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) op;
        Mutable<ILogicalExpression> exprRef = joinOp.getCondition();
        Mutable<ILogicalExpression> getItemExprRef = getSimilarityExpression(exprRef);
        if (getItemExprRef == null) {
            return false;
        }
        // Checks if the GET_ITEM function is on the one of the supported similarity-check functions.
        AbstractFunctionCallExpression getItemFuncExpr = (AbstractFunctionCallExpression) getItemExprRef.getValue();
        Mutable<ILogicalExpression> argRef = getItemFuncExpr.getArguments().get(0);
        AbstractFunctionCallExpression simFuncExpr = (AbstractFunctionCallExpression) argRef.getValue();
        if (!simFuncs.contains(simFuncExpr.getFunctionIdentifier())) {
            return false;
        }
        // Skips this rule based on annotations.
        if (simFuncExpr.getAnnotations().containsKey(IndexedNLJoinExpressionAnnotation.INSTANCE)) {
            return false;
        }

        // Gets both input branches of fuzzy join.
        List<Mutable<ILogicalOperator>> inputOps = joinOp.getInputs();
        ILogicalOperator leftInputOp = inputOps.get(0).getValue();
        ILogicalOperator rightInputOp = inputOps.get(1).getValue();

        List<Mutable<ILogicalExpression>> inputExprs = simFuncExpr.getArguments();
        if (inputExprs.size() != 3) {
            return false;
        }

        // Extracts Fuzzy similarity function.
        ILogicalExpression leftOperatingExpr = inputExprs.get(0).getValue();
        ILogicalExpression rightOperatingExpr = inputExprs.get(1).getValue();
        ILogicalExpression thresholdConstantExpr = inputExprs.get(2).getValue();

        // left and right expressions should be variables.
        if (leftOperatingExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE
                || rightOperatingExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE
                || thresholdConstantExpr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return false;
        }

        LogicalVariable inputVar0 = ((VariableReferenceExpression) leftOperatingExpr).getVariableReference();
        LogicalVariable inputVar1 = ((VariableReferenceExpression) rightOperatingExpr).getVariableReference();

        LogicalVariable leftInputVar;
        LogicalVariable rightInputVar;
        Collection<LogicalVariable> liveVars = new HashSet<>();
        VariableUtilities.getLiveVariables(leftInputOp, liveVars);
        if (liveVars.contains(inputVar0)) {
            leftInputVar = inputVar0;
            rightInputVar = inputVar1;
        } else {
            leftInputVar = inputVar1;
            rightInputVar = inputVar0;
        }
        // leftInputPKs in currrentPKs keeps all PKs derived from the left branch in the current similarity fuzzyjoin.
        List<LogicalVariable> leftInputPKs = findPrimaryKeysInSubplan(liveVars, context);
        liveVars.clear();
        VariableUtilities.getLiveVariables(rightInputOp, liveVars);
        // rightInputPKs in currentPKs keeps all PKs derived from the right branch in the current similarity fuzzyjoin.
        List<LogicalVariable> rightInputPKs = findPrimaryKeysInSubplan(liveVars, context);
        IAType leftType = (IAType) context.getOutputTypeEnvironment(leftInputOp).getVarType(leftInputVar);
        if (!isPrefixFuzzyJoin(context, leftInputOp, rightInputOp, rightInputVar, leftInputPKs, rightInputPKs,
                leftType)) {
            return false;
        }

        //
        // -- - FIRE - --
        //
        MetadataProvider metadataProvider = ((MetadataProvider) context.getMetadataProvider());
        // Steps 1 and 2. Generate the prefix-based fuzzy jon template.
        String aqlPlus = generateAqlTemplate(metadataProvider, joinOp, simFuncExpr, leftInputPKs, leftType,
                rightInputPKs, thresholdConstantExpr);
        // Steps 3 and 4. Generate the prefix-based fuzzy join subplan.
        ILogicalOperator outputOp = generatePrefixFuzzyJoinSubplan(context, metadataProvider, aqlPlus, leftInputOp,
                leftInputPKs, leftInputVar, rightInputOp, rightInputPKs, rightInputVar);

        // Step 5. Bind the plan to the parent op referred by the following opRef.
        SelectOperator extraSelect;
        if (getItemExprRef != exprRef) {
            // more than one join condition
            getItemExprRef.setValue(ConstantExpression.TRUE);
            switch (joinOp.getJoinKind()) {
                case INNER: {
                    extraSelect = new SelectOperator(exprRef, false, null);
                    extraSelect.setSourceLocation(exprRef.getValue().getSourceLocation());
                    extraSelect.getInputs().add(new MutableObject<>(outputOp));
                    outputOp = extraSelect;
                    break;
                }
                case LEFT_OUTER: {
                    LeftOuterJoinOperator topJoin = (LeftOuterJoinOperator) outputOp;
                    setConditionForLeftOuterJoin(topJoin, exprRef);
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
     * To handle multiple fuzzy-join conditions on a same pair of datasets, this rule checks the PKs in bottom-up way.
     * The previousPKs list incrementally maintains the PKs from a previous fuzzy-join operator's input branches.
     * In addition, the given fuzzy-join operator has been successfully translated into a prefix-based fuzzy join
     * sub-plan of the current fuzzy-join operator. There are two cases:
     * 1. If the previousPKs list contains the currentPKs list (the PKs from the input branches of the current
     * fuzzy-join operator), this means that the current fuzzy-join condition has no new input branch. This case
     * SHOULD BE regarded as a SELECT over one of the previous fuzzy-joins.
     * 2. Otherwise, we can apply this rule to the current fuzzy-join operator to a new prefix-based fuzzy-join plan.
     */
    private boolean isPrefixFuzzyJoin(IOptimizationContext context, ILogicalOperator leftInputOp,
            ILogicalOperator rightInputOp, LogicalVariable rightInputVar, List<LogicalVariable> leftInputPKs,
            List<LogicalVariable> rightInputPKs, IAType leftType) throws AlgebricksException {
        Collection<LogicalVariable> currentPKs = new HashSet<>();
        currentPKs.addAll(leftInputPKs);
        currentPKs.addAll(rightInputPKs);
        // If PKs derived from the both branches are SAME as that of a previous fuzzyjoin, we treat this fuzzyjoin
        // condition as a select over that fuzzyjoin.
        for (int i = 0; i < previousPKs.size(); i++) {
            if (previousPKs.get(i).containsAll(currentPKs) && currentPKs.containsAll(previousPKs.get(i))) {
                return false;
            }
        }

        //Suppose we want to query on the same dataset on the different fields, i.e. A.a1 ~= B.b1 AND A.a2 ~= B.b2
        //We conduct this query as a select over a fuzzyjoin based on the PK inclusion relationship.
        previousPKs.add(currentPKs);
        // Avoids the duplicated PK generation in findPrimaryKeysInSubplan, especially for multiway fuzzy join.
        // Refer to fj-dblp-csx-hybrid.aql in the optimized tests for example.
        IsomorphismUtilities.mergeHomogeneousPK(leftInputOp, leftInputPKs);
        // Fails if primary keys could not be inferred.
        if (leftInputPKs.isEmpty() || rightInputPKs.isEmpty()) {
            return false;
        }

        IAType rightType = (IAType) context.getOutputTypeEnvironment(rightInputOp).getVarType(rightInputVar);
        // left-hand side and right-hand side of fuzzyjoin should be the same type
        IAType actualLeftType = TypeComputeUtils.getActualType(leftType);
        IAType actualRightType = TypeComputeUtils.getActualType(rightType);
        if (!actualLeftType.deepEqual(actualRightType)) {
            return false;
        }
        return true;
    }

    private String generateAqlTemplate(MetadataProvider metadataProvider, AbstractBinaryJoinOperator joinOp,
            AbstractFunctionCallExpression simFuncExpr, List<LogicalVariable> leftInputPKs, IAType leftType,
            List<LogicalVariable> rightInputPKs, ILogicalExpression thresholdConstantExpr) throws AlgebricksException {
        FunctionIdentifier funcId = FuzzyUtils.getTokenizer(leftType.getTypeTag());
        String tokenizer = "";
        if (funcId != null) {
            tokenizer = funcId.getName();
        }

        String simFunction = FuzzyUtils.getSimFunction(simFuncExpr.getFunctionIdentifier());
        float simThreshold;
        ConstantExpression constExpr = (ConstantExpression) thresholdConstantExpr;
        AsterixConstantValue constVal = (AsterixConstantValue) constExpr.getValue();
        if (constVal.getObject().getType().equals(BuiltinType.AFLOAT)) {
            simThreshold = ((AFloat) constVal.getObject()).getFloatValue();
        } else {
            simThreshold = FuzzyUtils.getSimThreshold(metadataProvider);
        }

        // finalize AQL+ query
        String prepareJoin;
        switch (joinOp.getJoinKind()) {
            case INNER:
                prepareJoin = AQLPLUS_INNER_JOIN + AQLPLUS;
                break;
            case LEFT_OUTER:
                prepareJoin = AQLPLUS_LEFTOUTER_JOIN + AQLPLUS;
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_INVALID_EXPRESSION);
        }
        String groupByLeft = "";
        String joinCondLeft = "";
        // Step2. By default, we triggered the prefix fuzzy join strategy, which needs to initialize the mapping from
        // the shared token to the actual tuples of the both sides. left(right)InputPKs is used to extract those
        // mappings from prefix tokens to tuples.
        for (int i = 0; i < leftInputPKs.size(); i++) {
            if (i > 0) {
                groupByLeft += ", ";
                joinCondLeft += " and ";
            }
            groupByLeft += String.format(Locale.US, GROUPBY_LEFT, i, i);
            joinCondLeft += String.format(Locale.US, JOIN_COND_LEFT, i, i);
        }

        String groupByRight = "";
        String joinCondRight = "";
        for (int i = 0; i < rightInputPKs.size(); i++) {
            if (i > 0) {
                groupByRight += ", ";
                joinCondRight += " and ";
            }
            groupByRight += String.format(Locale.US, GROUPBY_RIGHT, i, i);
            joinCondRight += String.format(Locale.US, JOIN_COND_RIGHT, i, i);
        }
        return String.format(Locale.US, prepareJoin, tokenizer, tokenizer, simFunction, simThreshold, tokenizer,
                tokenizer, simFunction, simThreshold, simFunction, simThreshold, simThreshold, groupByLeft,
                groupByRight, joinCondRight, joinCondLeft);
    }

    private ILogicalOperator generatePrefixFuzzyJoinSubplan(IOptimizationContext context,
            MetadataProvider metadataProvider, String aqlPlus, ILogicalOperator leftInputOp,
            List<LogicalVariable> leftInputPKs, LogicalVariable leftInputVar, ILogicalOperator rightInputOp,
            List<LogicalVariable> rightInputPKs, LogicalVariable rightInputVar) throws AlgebricksException {
        // Step3. Translate the tokenizer, join condition and group by (shared token) as shown
        // in the above AQLPLUS template.
        Counter counter = new Counter(context.getVarCounter());
        // The translator will compile metadata internally. Run this compilation
        // under the same transaction id as the "outer" compilation.
        AqlPlusExpressionToPlanTranslator translator = new AqlPlusExpressionToPlanTranslator(metadataProvider, counter);

        LogicalOperatorDeepCopyWithNewVariablesVisitor copyVisitor =
                new LogicalOperatorDeepCopyWithNewVariablesVisitor(context, context);

        // Step3.1. Substitute the variable references of the above AQLPLUS template with
        // the actually attached variables.
        translator.addOperatorToMetaScope(new VarIdentifier("##LEFT_0"), leftInputOp);
        translator.addVariableToMetaScope(new VarIdentifier("$$LEFT_0"), leftInputVar);
        for (int i = 0; i < leftInputPKs.size(); i++) {
            translator.addVariableToMetaScope(new VarIdentifier("$$LEFTPK_0_" + i), leftInputPKs.get(i));
        }

        // Step3.2. right side again.
        translator.addOperatorToMetaScope(new VarIdentifier("##RIGHT_0"), rightInputOp);
        translator.addVariableToMetaScope(new VarIdentifier("$$RIGHT_0"), rightInputVar);
        for (int i = 0; i < rightInputPKs.size(); i++) {
            translator.addVariableToMetaScope(new VarIdentifier("$$RIGHTPK_0_" + i), rightInputPKs.get(i));
        }

        // Step3.3. the suffix 0-3 is used for identifying the different level of variable references.
        ILogicalOperator leftInputOpCopy = copyVisitor.deepCopy(leftInputOp);
        translator.addOperatorToMetaScope(new VarIdentifier("##LEFT_1"), leftInputOpCopy);
        LogicalVariable leftInputVarCopy = copyVisitor.varCopy(leftInputVar);
        translator.addVariableToMetaScope(new VarIdentifier("$$LEFT_1"), leftInputVarCopy);
        for (int i = 0; i < leftInputPKs.size(); i++) {
            leftInputVarCopy = copyVisitor.varCopy(leftInputPKs.get(i));
            translator.addVariableToMetaScope(new VarIdentifier("$$LEFTPK_1_" + i), leftInputVarCopy);
        }
        copyVisitor.updatePrimaryKeys(context);
        copyVisitor.reset();

        // Notice: pick side to run Stage 1, currently always picks RIGHT side. It means that the right side will
        // produce the token order as well as its own token list.
        for (int i = SUBSET_RIGHT_INDEX; i <= LENGTH_RIGHT_INDEX; i++) {
            translator.addOperatorToMetaScope(new VarIdentifier("##RIGHT_" + i), copyVisitor.deepCopy(rightInputOp));
            LogicalVariable rightInputVarCopy = copyVisitor.varCopy(rightInputVar);
            translator.addVariableToMetaScope(new VarIdentifier("$$RIGHT_" + i), rightInputVarCopy);
            for (int j = 0; j < rightInputPKs.size(); j++) {
                rightInputVarCopy = copyVisitor.varCopy(rightInputPKs.get(j));
                translator.addVariableToMetaScope(new VarIdentifier("$$RIGHTPK_" + i + "_" + j), rightInputVarCopy);
            }
            copyVisitor.updatePrimaryKeys(context);
            copyVisitor.reset();
        }
        counter.set(context.getVarCounter());

        AQLPlusParser parser = new AQLPlusParser(new StringReader(aqlPlus));
        parser.initScope();
        parser.setVarCounter(counter);
        List<Clause> clauses;
        try {
            clauses = parser.Clauses();
        } catch (ParseException e) {
            throw CompilationException.create(ErrorCode.COMPILATION_TRANSLATION_ERROR, e);
        }

        // Step 4. The essential substitution with translator.
        ILogicalPlan plan;
        try {
            plan = translator.translate(clauses);
        } catch (CompilationException e) {
            throw CompilationException.create(ErrorCode.COMPILATION_TRANSLATION_ERROR, e);
        }
        context.setVarCounter(counter.get());

        return plan.getRoots().get(0).getValue();
    }

    // Since the generatePrefixFuzzyJoinSubplan generates the prefix-based join operators for the partial simJoin
    // of expRef, we need to add the full condition expRef\getItemExprRef into the top-level operator of the plan.
    // Notice: Any composite select on leftOuterJoin with fuzzyjoin condition inlined can be regarded as its example.
    // Example: leftouterjoin-probe-pidx-with-join-edit-distance-check-idx_01.aql or with more extra conditions inlined.
    private void setConditionForLeftOuterJoin(LeftOuterJoinOperator topJoin, Mutable<ILogicalExpression> expRef) {
        // Combine the conditions of top join of aqlplus plan and the original join
        AbstractFunctionCallExpression andFunc =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(AlgebricksBuiltinFunctions.AND));

        List<Mutable<ILogicalExpression>> conjs = new ArrayList<>();
        if (topJoin.getCondition().getValue().splitIntoConjuncts(conjs)) {
            andFunc.getArguments().addAll(conjs);
        } else {
            andFunc.getArguments().add(new MutableObject<>(topJoin.getCondition().getValue()));
        }

        List<Mutable<ILogicalExpression>> conjs2 = new ArrayList<>();
        if (expRef.getValue().splitIntoConjuncts(conjs2)) {
            andFunc.getArguments().addAll(conjs2);
        } else {
            andFunc.getArguments().add(expRef);
        }
        topJoin.getCondition().setValue(andFunc);
    }

    /**
     * Look for GET_ITEM function call.
     */
    private Mutable<ILogicalExpression> getSimilarityExpression(Mutable<ILogicalExpression> exprRef) {
        ILogicalExpression exp = exprRef.getValue();
        if (exp.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) exp;
            if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.GET_ITEM)) {
                return exprRef;
            }
            if (funcExpr.getFunctionIdentifier().equals(AlgebricksBuiltinFunctions.AND)) {
                for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
                    Mutable<ILogicalExpression> expRefRet = getSimilarityExpression(arg);
                    if (expRefRet != null) {
                        return expRefRet;
                    }
                }
            }
        }
        return null;
    }

    // To extract all the PKs of the liveVars referenced by on of the fuzzyjoin branch branch.
    private List<LogicalVariable> findPrimaryKeysInSubplan(Collection<LogicalVariable> liveVars,
            IOptimizationContext context) {
        Collection<LogicalVariable> primaryKeys = new HashSet<>();
        for (LogicalVariable var : liveVars) {
            List<LogicalVariable> pks = context.findPrimaryKey(var);
            if (pks != null) {
                primaryKeys.addAll(pks);
            }
        }
        if (primaryKeys.isEmpty()) {
            return new ArrayList<>();
        }
        return new ArrayList<>(primaryKeys);
    }
}
