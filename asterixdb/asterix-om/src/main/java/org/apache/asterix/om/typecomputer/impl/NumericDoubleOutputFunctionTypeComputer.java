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
import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public class NumericDoubleOutputFunctionTypeComputer extends AbstractResultTypeComputer {

    public static final NumericDoubleOutputFunctionTypeComputer INSTANCE =
            new NumericDoubleOutputFunctionTypeComputer();

    private NumericDoubleOutputFunctionTypeComputer() {
    }

    @Override
    protected void checkArgType(String funcName, int argIndex, IAType type) throws AlgebricksException {
        ATypeTag tag = type.getTypeTag();
        switch (tag) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
                break;
            default:
                throw new TypeMismatchException(funcName, argIndex, tag, ATypeTag.INT8, ATypeTag.INT16, ATypeTag.INT32,
                        ATypeTag.INT64, ATypeTag.FLOAT, ATypeTag.DOUBLE);
        }
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        return BuiltinType.ADOUBLE;
    }
}
