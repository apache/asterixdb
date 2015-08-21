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
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class NonTaggedSwitchCaseComputer implements IResultTypeComputer {

    private static final String errMsg1 = "switch case should have at least 3 parameters ";
    private static final String errMsg2 = "every case expression should have the same return type";
    private static final String errMsg3 = "swith conditon type should be compatible with each case type";

    public static IResultTypeComputer INSTANCE = new NonTaggedSwitchCaseComputer();

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        if (fce.getArguments().size() < 3)
            throw new AlgebricksException(errMsg1);

        TypeCompatibilityChecker tcc = new TypeCompatibilityChecker();
        for (int i = 2; i < fce.getArguments().size(); i += 2) {
            IAType ti = (IAType) env.getType(fce.getArguments().get(i).getValue());
            tcc.addPossibleType(ti);
        }
        IAType valueType = tcc.getCompatibleType();
        if (valueType == null) {
            throw new AlgebricksException(errMsg2);
        }

        IAType switchType = (IAType) env.getType(fce.getArguments().get(0).getValue());
        tcc.reset();
        tcc.addPossibleType(switchType);
        for (int i = 1; i < fce.getArguments().size(); i += 2) {
            IAType ti = (IAType) env.getType(fce.getArguments().get(i).getValue());
            tcc.addPossibleType(ti);
        }
        IAType caseType = tcc.getCompatibleType();
        if (caseType == null) {
            throw new AlgebricksException(errMsg3);
        }
        return valueType;
    }
}
