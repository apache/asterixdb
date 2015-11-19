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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.AString;
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

public class FieldAccessNestedResultType implements IResultTypeComputer {
    public static final FieldAccessNestedResultType INSTANCE = new FieldAccessNestedResultType();

    private FieldAccessNestedResultType() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
        Object obj;
        obj = env.getType(f.getArguments().get(0).getValue());
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
            throw new AlgebricksException("Typing error: expecting a constant value, found " + ce + " instead.");
        }
        IAObject v = ((AsterixConstantValue) ce.getValue()).getObject();
        List<String> fieldPath = new ArrayList<String>();
        if (v.getType().getTypeTag() == ATypeTag.ORDEREDLIST) {
            for (int i = 0; i < ((AOrderedList) v).size(); i++) {
                fieldPath.add(((AString) ((AOrderedList) v).getItem(i)).getStringValue());
            }
        } else if (v.getType().getTypeTag() == ATypeTag.STRING) {
            fieldPath.add(((AString) v).getStringValue());
        } else {
            throw new AlgebricksException("Typing error: expecting a String, found " + ce + " instead.");
        }
        try {
            IAType subType = t0.getSubFieldType(fieldPath);
            if (subType != null) {
                return subType;
            } else {
                // Open field. Type can only be determined at runtime.
                return BuiltinType.ANY;
            }
        } catch (IOException e) {
            throw new AlgebricksException("FieldPath was invalid.");
        }
    }

}
