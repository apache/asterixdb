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

package org.apache.asterix.optimizer.rules.am.array;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.ArrayIndexUtil;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.optimizer.rules.am.OptimizableOperatorSubTree;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;

public class ArrayIndexStructureMatcher implements ArrayIndexUtil.TypeTrackerCommandExecutor {
    private final List<AbstractLogicalOperator> logicalOperators = new ArrayList<>();
    private int lastMatchedPosition = -1;
    private boolean isStructureMatched = true;

    private LogicalVariable varFromParent;
    private IAType typeFromParent;

    @Override
    public void executeActionOnEachArrayStep(ARecordType startingStepRecordType, IAType workingType,
            List<String> fieldName, boolean isFirstArrayStep, boolean isLastUnnestInIntermediateStep) {
        // Avoiding exploring, we could not match an earlier array step.
        if (!isStructureMatched) {
            return;
        }

        // Match our field name and a set of ASSIGNs.
        boolean isStructureMatchedForThisStep = matchAssignVarAndFieldName(startingStepRecordType, fieldName);

        // Match an UNNEST operator with the tail of the previously matched ASSIGN.
        if (isStructureMatchedForThisStep) {
            isStructureMatchedForThisStep = matchUnnestVar();
        }

        // Update our flags.
        isStructureMatched = isStructureMatched && isStructureMatchedForThisStep;
    }

    @Override
    public void executeActionOnFinalArrayStep(Index.ArrayIndexElement workingElement, ARecordType baseRecordType,
            ARecordType startingStepRecordType, List<String> fieldName, boolean isNonArrayStep,
            boolean requiresOnlyOneUnnest) {
        if (isNonArrayStep) {
            isStructureMatched = isStructureMatched && matchAssignVarAndFieldName(startingStepRecordType, fieldName);
        }

        if (!isStructureMatched) {
            // If no match is found, signal this to our caller.
            varFromParent = null;
            lastMatchedPosition = -1;
        }
    }

    public void reset(LogicalVariable assignVar, OptimizableOperatorSubTree subTree) {
        varFromParent = assignVar;
        typeFromParent = null;

        // We start by assuming that the structure is matched, and try to find steps where this does not hold.
        isStructureMatched = true;

        // Build our list from the ASSIGNs and UNNESTs collected in our subtree.
        lastMatchedPosition = -1;
        logicalOperators.clear();
        logicalOperators.addAll(subTree.getAssignsAndUnnests());
        Collections.reverse(logicalOperators);
    }

    public LogicalVariable getEndVar() {
        return varFromParent;
    }

    public ILogicalOperator getEndOperator() {
        return (lastMatchedPosition == -1) ? null : logicalOperators.get(lastMatchedPosition);
    }

    private boolean matchUnnestVar() {
        boolean isStructureMatchedFoundForThisStep = false;
        AbstractLogicalOperator workingOp;
        int searchPosition = lastMatchedPosition + 1;

        // Match the UNNEST variable. Ignore any ASSIGNs we run into here.
        while (!isStructureMatchedFoundForThisStep && searchPosition < logicalOperators.size()) {
            workingOp = logicalOperators.get(searchPosition);

            if (workingOp.getOperatorTag().equals(LogicalOperatorTag.UNNEST)) {
                UnnestOperator workingOpAsUnnest = (UnnestOperator) workingOp;
                List<LogicalVariable> expressionUsedVariables = new ArrayList<>();
                ILogicalExpression unnestExpr = workingOpAsUnnest.getExpressionRef().getValue();
                unnestExpr.getUsedVariables(expressionUsedVariables);

                if (expressionUsedVariables.contains(varFromParent)) {
                    varFromParent = workingOpAsUnnest.getVariable();
                    lastMatchedPosition = searchPosition;
                    isStructureMatchedFoundForThisStep = true;

                }
            }
            searchPosition++;
        }
        return isStructureMatchedFoundForThisStep;
    }

    private boolean matchAssignVarAndFieldName(ARecordType recordType, List<String> fieldName) {
        boolean isStructureMatchedForThisStep = false;
        AbstractLogicalOperator workingOp;
        int searchPosition;

        typeFromParent = recordType;
        for (String fieldPart : fieldName) {
            searchPosition = lastMatchedPosition + 1;
            isStructureMatchedForThisStep = false;

            // Match the ASSIGN variable + field name. Ignore the UNNESTs we find here.
            while (!isStructureMatchedForThisStep && searchPosition < logicalOperators.size()) {
                workingOp = logicalOperators.get(searchPosition);
                if (workingOp.getOperatorTag().equals(LogicalOperatorTag.ASSIGN)) {

                    // Keep track of our type between matching field part calls.
                    ARecordType typeForFieldPart =
                            (typeFromParent instanceof ARecordType) ? (ARecordType) typeFromParent : recordType;

                    // Match the specific field part.
                    if (matchAssignVarAndFieldPart((AssignOperator) workingOp, typeForFieldPart, fieldPart)) {
                        isStructureMatchedForThisStep = true;
                        lastMatchedPosition = searchPosition;
                    }
                }
                searchPosition++;
            }
        }
        return isStructureMatchedForThisStep;
    }

    private boolean matchAssignVarAndFieldPart(AssignOperator workingOp, ARecordType recordType, String fieldPart) {
        final List<LogicalVariable> expressionUsedVariables = new ArrayList<>();
        for (int j = 0; j < workingOp.getExpressions().size(); j++) {
            ILogicalExpression assignExpr = workingOp.getExpressions().get(j).getValue();
            assignExpr.getUsedVariables(expressionUsedVariables);

            boolean isVarInExpression = expressionUsedVariables.contains(varFromParent);
            boolean isVarInOutput = workingOp.getVariables().get(j).equals(varFromParent);
            if (isVarInExpression || isVarInOutput) {
                // We have found a potential match. We must now map the field names from the assign to what is actually
                // specified in the index.

                if (assignExpr.getExpressionTag().equals(LogicalExpressionTag.FUNCTION_CALL)) {
                    ScalarFunctionCallExpression assignFunc = (ScalarFunctionCallExpression) assignExpr;
                    String fieldNameFromAssign = null;

                    if (assignFunc.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
                        // Search for the field name inside the expression itself.
                        ConstantExpression assignNameExpr =
                                (ConstantExpression) assignFunc.getArguments().get(1).getValue();
                        IAObject assignNameObj = ((AsterixConstantValue) assignNameExpr.getValue()).getObject();
                        fieldNameFromAssign = ((AString) assignNameObj).getStringValue();

                    } else if (assignFunc.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_INDEX)) {
                        // Search for the field name using the type we are tracking.
                        ConstantExpression assignIndexExpr =
                                (ConstantExpression) assignFunc.getArguments().get(1).getValue();
                        IAObject assignIndexObj = ((AsterixConstantValue) assignIndexExpr.getValue()).getObject();
                        int assignIndex = ((AInt32) assignIndexObj).getIntegerValue();
                        fieldNameFromAssign = recordType.getFieldNames()[assignIndex];
                        typeFromParent = recordType.getFieldTypes()[assignIndex];
                    }

                    if (fieldNameFromAssign != null && fieldNameFromAssign.equals(fieldPart)) {
                        // We have found a match.
                        varFromParent = workingOp.getVariables().get(j);
                        return true;
                    }
                }
            }
        }

        return false;
    }
}
