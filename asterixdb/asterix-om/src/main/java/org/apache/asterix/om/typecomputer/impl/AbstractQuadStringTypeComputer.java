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
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

/**
 * @author Xiaoyu Ma
 */
public abstract class AbstractQuadStringTypeComputer implements IResultTypeComputer {

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        if (fce.getArguments().size() < 4)
            throw new AlgebricksException("Wrong Argument Number.");
        ILogicalExpression arg0 = fce.getArguments().get(0).getValue();
        ILogicalExpression arg1 = fce.getArguments().get(1).getValue();
        ILogicalExpression arg2 = fce.getArguments().get(2).getValue();
        ILogicalExpression arg3 = fce.getArguments().get(3).getValue();
        IAType t0, t1, t2, t3;
        try {
            t0 = (IAType) env.getType(arg0);
            t1 = (IAType) env.getType(arg1);
            t2 = (IAType) env.getType(arg2);
            t3 = (IAType) env.getType(arg3);
        } catch (AlgebricksException e) {
            throw new AlgebricksException(e);
        }
        if ((t0.getTypeTag() != ATypeTag.NULL && t0.getTypeTag() != ATypeTag.STRING)
                || (t1.getTypeTag() != ATypeTag.NULL && t1.getTypeTag() != ATypeTag.STRING)
                || (t2.getTypeTag() != ATypeTag.NULL && t2.getTypeTag() != ATypeTag.STRING)
                || (t3.getTypeTag() != ATypeTag.NULL && t3.getTypeTag() != ATypeTag.STRING)) {
            throw new NotImplementedException("Expects String Type.");
        }

        return getResultType(t0, t1, t2, t3);
    }

    public abstract IAType getResultType(IAType t0, IAType t1, IAType t2, IAType t3);
}
