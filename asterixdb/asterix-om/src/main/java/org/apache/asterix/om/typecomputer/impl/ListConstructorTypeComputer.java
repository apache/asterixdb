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
import org.apache.asterix.om.typecomputer.base.TypeCastUtils;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public abstract class ListConstructorTypeComputer implements IResultTypeComputer {

    protected ListConstructorTypeComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;

        /**
         * if type has been top-down propagated, use the enforced type
         */
        IAType reqType = TypeCastUtils.getRequiredType(f);
        if (reqType != null) {
            return reqType;
        }
        final IAType currentType = computeContentType(env, f);
        return getListType(currentType == null ? BuiltinType.ANY : currentType);
    }

    private IAType computeContentType(IVariableTypeEnvironment env, AbstractFunctionCallExpression f)
            throws AlgebricksException {
        IAType currentType = null;
        for (int k = 0; k < f.getArguments().size(); k++) {
            IAType type = (IAType) env.getType(f.getArguments().get(k).getValue());
            if (type.getTypeTag() == ATypeTag.UNION || (currentType != null && !currentType.equals(type))) {
                return null;
            }
            currentType = type;
        }
        return currentType;
    }

    protected abstract IAType getListType(IAType itemType);

}
