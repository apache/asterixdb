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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.typecomputer.base.TypeCastUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeHelper;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class OpenRecordConstructorResultType implements IResultTypeComputer {

    public static final OpenRecordConstructorResultType INSTANCE = new OpenRecordConstructorResultType();

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;

        /**
         * if type has been top-down propagated, use the enforced type
         */
        ARecordType type = (ARecordType) TypeCastUtils.getRequiredType(f);
        if (type != null) {
            return type;
        }

        Iterator<Mutable<ILogicalExpression>> argIter = f.getArguments().iterator();
        List<String> namesList = new ArrayList<>();
        List<IAType> typesList = new ArrayList<>();
        // The following set of names do not belong to the closed part,
        // but are additional possible field names. For example, a field "foo" with type
        // ANY cannot be in the closed part, but "foo" is a possible field name.
        Set<String> allPossibleAdditionalFieldNames = new HashSet<>();
        boolean canProvideAdditionFieldInfo = true;
        boolean isOpen = false;
        while (argIter.hasNext()) {
            ILogicalExpression e1 = argIter.next().getValue();
            ILogicalExpression e2 = argIter.next().getValue();
            IAType t2 = (IAType) env.getType(e2);
            String fieldName = ConstantExpressionUtil.getStringConstant(e1);
            if (fieldName != null && t2 != null && TypeHelper.isClosed(t2)) {
                if (namesList.contains(fieldName)) {
                    throw new CompilationException(ErrorCode.DUPLICATE_FIELD_NAME, f.getSourceLocation(), fieldName);
                }
                namesList.add(fieldName);
                if (t2.getTypeTag() == ATypeTag.UNION) {
                    AUnionType unionType = (AUnionType) t2;
                    t2 = AUnionType.createUnknownableType(unionType.getActualType());
                }
                typesList.add(t2);
            } else {
                if (canProvideAdditionFieldInfo && fieldName != null) {
                    allPossibleAdditionalFieldNames.add(fieldName);
                } else {
                    canProvideAdditionFieldInfo = false;
                }
                isOpen = true;
            }
        }
        String[] fieldNames = namesList.toArray(new String[0]);
        IAType[] fieldTypes = typesList.toArray(new IAType[0]);
        return canProvideAdditionFieldInfo
                ? new ARecordType(null, fieldNames, fieldTypes, isOpen, allPossibleAdditionalFieldNames)
                : new ARecordType(null, fieldNames, fieldTypes, isOpen);
    }
}
