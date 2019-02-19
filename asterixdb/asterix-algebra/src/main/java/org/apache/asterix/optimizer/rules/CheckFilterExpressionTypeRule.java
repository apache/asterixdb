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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.util.LogRedactionUtil;

/**
 * This rule is to check if all the filter expression are of the boolean type or a possible (determined
 * at the runtime) boolean type.
 * If that is not the case, an exception should be thrown.
 *
 * @author yingyib
 */
public class CheckFilterExpressionTypeRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        SelectOperator select = (SelectOperator) op;
        ILogicalExpression condition = select.getCondition().getValue();

        // Get the output type environment
        IVariableTypeEnvironment env = context.getOutputTypeEnvironment(select);

        IAType condType = (IAType) env.getType(condition);
        if (condType.getTypeTag() != ATypeTag.BOOLEAN && condType.getTypeTag() != ATypeTag.ANY
                && !isPossibleBoolean(condType)) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, condition.getSourceLocation(),
                    "The select condition " + LogRedactionUtil.userData(condition.toString()) + " should be of the "
                            + "boolean type.");
        }
        return false;
    }

    /**
     * Check if the type is optional boolean or not
     *
     * @param type
     * @return true if it is; false otherwise.
     */
    private boolean isPossibleBoolean(IAType type) {
        IAType checkingType = type;
        while (NonTaggedFormatUtil.isOptional(checkingType)) {
            IAType actualType = ((AUnionType) checkingType).getActualType();
            if (actualType.getTypeTag() == ATypeTag.BOOLEAN || actualType.getTypeTag() == ATypeTag.ANY) {
                return true;
            }
            checkingType = actualType;
        }
        return false;
    }

}
