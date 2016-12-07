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

import org.apache.asterix.om.exceptions.TypeMismatchException;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class SubsetCollectionTypeComputer implements IResultTypeComputer {

    public static final SubsetCollectionTypeComputer INSTANCE = new SubsetCollectionTypeComputer();

    private SubsetCollectionTypeComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env, IMetadataProvider<?, ?> mp)
            throws AlgebricksException {
        AbstractFunctionCallExpression fun = (AbstractFunctionCallExpression) expression;
        String funcName = fun.getFunctionIdentifier().getName();

        IAType t = (IAType) env.getType(fun.getArguments().get(0).getValue());
        ATypeTag actualTypeTag = t.getTypeTag();
        switch (actualTypeTag) {
            case UNORDEREDLIST:
            case ORDEREDLIST: {
                AbstractCollectionType act = (AbstractCollectionType) t;
                return act.getItemType();
            }
            case UNION: {
                AUnionType ut = (AUnionType) t;
                if (!ut.isUnknownableType()) {
                    throw new TypeMismatchException(funcName, 0, actualTypeTag, ATypeTag.UNORDEREDLIST,
                            ATypeTag.ORDEREDLIST);
                }
                IAType t2 = ut.getActualType();
                ATypeTag tag2 = t2.getTypeTag();
                if (tag2 == ATypeTag.UNORDEREDLIST || tag2 == ATypeTag.ORDEREDLIST) {
                    AbstractCollectionType act = (AbstractCollectionType) t2;
                    return act.getItemType();
                }
                throw new TypeMismatchException(funcName, 0, actualTypeTag, ATypeTag.UNORDEREDLIST,
                        ATypeTag.ORDEREDLIST);
            }
            case ANY:
                return BuiltinType.ANY;
            default:
                throw new TypeMismatchException(funcName, 0, actualTypeTag, ATypeTag.UNORDEREDLIST,
                        ATypeTag.ORDEREDLIST);
        }
    }
}
