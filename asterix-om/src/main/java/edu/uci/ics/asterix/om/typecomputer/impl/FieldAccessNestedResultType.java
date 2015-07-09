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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.om.base.AOrderedList;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class FieldAccessNestedResultType implements IResultTypeComputer {
    private static final long serialVersionUID = 1L;
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
