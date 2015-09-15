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
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class SubstringTypeComputer implements IResultTypeComputer {
    public static final SubstringTypeComputer INSTANCE = new SubstringTypeComputer();

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        if (fce.getArguments().size() < 3)
            throw new AlgebricksException("Wrong Argument Number.");
        ILogicalExpression arg0 = fce.getArguments().get(0).getValue();
        ILogicalExpression arg1 = fce.getArguments().get(1).getValue();
        ILogicalExpression arg2 = fce.getArguments().get(2).getValue();
        IAType t0, t1, t2;
        try {
            t0 = (IAType) env.getType(arg0);
            t1 = (IAType) env.getType(arg1);
            t2 = (IAType) env.getType(arg2);
        } catch (AlgebricksException e) {
            throw new AlgebricksException(e);
        }

        ATypeTag tag0, tag1, tag2;
        if (NonTaggedFormatUtil.isOptional(t0))
            tag0 = ((AUnionType) t0).getNullableType().getTypeTag();
        else
            tag0 = t0.getTypeTag();

        if (NonTaggedFormatUtil.isOptional(t1))
            tag1 = ((AUnionType) t1).getNullableType().getTypeTag();
        else
            tag1 = t1.getTypeTag();

        if (NonTaggedFormatUtil.isOptional(t2))
            tag2 = ((AUnionType) t2).getNullableType().getTypeTag();
        else
            tag2 = t2.getTypeTag();

        if (tag0 == ATypeTag.ANY || tag1 == ATypeTag.ANY || tag2 == ATypeTag.ANY)
            return BuiltinType.ANY;

        if (tag0 != ATypeTag.NULL && tag0 != ATypeTag.STRING) {
            throw new AlgebricksException("First argument should be String Type.");
        }

        if (tag1 != ATypeTag.NULL && tag1 != ATypeTag.INT8 && tag1 != ATypeTag.INT16 && tag1 != ATypeTag.INT32
                && tag1 != ATypeTag.INT64) {
            throw new AlgebricksException("Second argument should be integer Type.");
        }

        if (tag2 != ATypeTag.NULL && tag2 != ATypeTag.INT8 && tag2 != ATypeTag.INT16 && tag2 != ATypeTag.INT32
                && tag2 != ATypeTag.INT64) {
            throw new AlgebricksException("Third argument should be integer Type.");
        }

        return BuiltinType.ASTRING;
    }
}
