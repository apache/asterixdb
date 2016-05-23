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
package org.apache.asterix.om.typecomputer.impl;

import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class SwitchCaseComputer implements IResultTypeComputer {

    private static final String ERR_MSG = "switch case should have at least 3 parameters";

    public static final IResultTypeComputer INSTANCE = new SwitchCaseComputer();

    private SwitchCaseComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        if (fce.getArguments().size() < 3) {
            throw new AlgebricksException(ERR_MSG);
        }

        IAType currentType = null;
        boolean any = false;
        boolean missable = false;
        for (int i = 2; i < fce.getArguments().size(); i += 2) {
            IAType type = (IAType) env.getType(fce.getArguments().get(i).getValue());
            if (type.getTypeTag() == ATypeTag.UNION) {
                type = ((AUnionType) type).getActualType();
                missable = true;
            }
            if (currentType != null && !type.equals(currentType)) {
                any = true;
                break;
            }
            currentType = type;
        }
        return any ? BuiltinType.ANY : missable ? AUnionType.createMissableType(currentType) : currentType;
    }
}
