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

import java.util.Iterator;

import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.typecomputer.base.TypeComputerUtilities;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class ClosedRecordConstructorResultType implements IResultTypeComputer {

    public static final ClosedRecordConstructorResultType INSTANCE = new ClosedRecordConstructorResultType();

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;

        /**
         * if type has been top-down propagated, use the enforced type
         */
        ARecordType type = (ARecordType) TypeComputerUtilities.getRequiredType(f);
        if (type != null)
            return type;

        int n = f.getArguments().size() / 2;
        String[] fieldNames = new String[n];
        IAType[] fieldTypes = new IAType[n];
        int i = 0;
        Iterator<Mutable<ILogicalExpression>> argIter = f.getArguments().iterator();
        while (argIter.hasNext()) {
            ILogicalExpression e1 = argIter.next().getValue();
            if (e1.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                ConstantExpression nameExpr = (ConstantExpression) e1;
                fieldNames[i] = ((AString) ((AsterixConstantValue) nameExpr.getValue()).getObject()).getStringValue();
            } else {
                throw new AlgebricksException(
                        "Field name " + i + "(" + e1 + ") in call to closed-record-constructor is not a constant.");
            }
            ILogicalExpression e2 = argIter.next().getValue();
            fieldTypes[i] = (IAType) env.getType(e2);
            i++;
        }
        return new ARecordType(null, fieldNames, fieldTypes, false);
    }
}
