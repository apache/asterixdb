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

package edu.uci.ics.asterix.om.typecomputer.impl;

import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

/**
 * The type computer for concat-not-null.
 * Note that this function is only used for the if-then-else clause.
 *
 * @author yingyib
 */
public class ConcatNonNullTypeComputer implements IResultTypeComputer {

    public static final ConcatNonNullTypeComputer INSTANCE = new ConcatNonNullTypeComputer();

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
        if (f.getArguments().size() < 1) {
            return BuiltinType.ANULL;
        }

        TypeCompatibilityChecker tcc = new TypeCompatibilityChecker();
        for (int i = 0; i < f.getArguments().size(); i++) {
            ILogicalExpression arg = f.getArguments().get(i).getValue();
            IAType type = (IAType) env.getType(arg);
            tcc.addPossibleType(type);
        }

        IAType result = tcc.getCompatibleType();
        if (result == null) {
            throw new AlgebricksException("The two branches of the if-else clause should return the same type.");
        }
        return result;
    }
}
