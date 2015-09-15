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

import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class FieldAccessByIndexResultType implements IResultTypeComputer {

    public static final FieldAccessByIndexResultType INSTANCE = new FieldAccessByIndexResultType();

    private FieldAccessByIndexResultType() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
        Object obj;
        try {
            obj = env.getType(f.getArguments().get(0).getValue());
        } catch (AlgebricksException e) {
            throw new AlgebricksException(e);
        }
        if (obj == null) {
            return null;
        }
        IAType type0 = (IAType) obj;
        ARecordType t0 = NonTaggedFieldAccessByNameResultType.getRecordTypeFromType(type0, expression);
        if (t0 == null) {
            return BuiltinType.ANY;
        }
        ILogicalExpression arg1 = f.getArguments().get(1).getValue();
        if (arg1.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return BuiltinType.ANY;
        }
        ConstantExpression ce = (ConstantExpression) arg1;
        if (!(ce.getValue() instanceof AsterixConstantValue)) {
            throw new AlgebricksException("Typing error: expecting an integer, found " + ce + " instead.");
        }
        IAObject v = ((AsterixConstantValue) ce.getValue()).getObject();
        if (v.getType().getTypeTag() != ATypeTag.INT32) {
            throw new AlgebricksException("Typing error: expecting an INT32, found " + ce + " instead.");
        }
        int pos = ((AInt32) v).getIntegerValue();
        return t0.getFieldTypes()[pos];
    }

}
