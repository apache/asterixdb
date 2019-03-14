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

package org.apache.asterix.om.typecomputer.base;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.om.exceptions.IncompatibleTypeException;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class TypeCastUtils {

    private TypeCastUtils() {
    }

    public static boolean setRequiredAndInputTypes(AbstractFunctionCallExpression expr, IAType requiredType,
            IAType inputType) throws CompilationException {
        boolean changed = false;
        Object[] opaqueParameters = expr.getOpaqueParameters();
        if (opaqueParameters == null) {
            opaqueParameters = new Object[2];
            opaqueParameters[0] = requiredType;
            opaqueParameters[1] = inputType;
            ATypeTag requiredTypeTag = requiredType.getTypeTag();
            ATypeTag actualTypeTag = TypeComputeUtils.getActualType(inputType).getTypeTag();
            if (!ATypeHierarchy.isCompatible(requiredTypeTag, actualTypeTag)) {
                FunctionIdentifier funcId = expr.getFunctionIdentifier();
                throw new IncompatibleTypeException(expr.getSourceLocation(), funcId, actualTypeTag, requiredTypeTag);
            }
            expr.setOpaqueParameters(opaqueParameters);
            changed = true;
        }
        return changed;
    }

    public static void resetRequiredAndInputTypes(AbstractFunctionCallExpression expr) {
        expr.setOpaqueParameters(null);
    }

    public static IAType getRequiredType(AbstractFunctionCallExpression expr) {
        Object[] type = expr.getOpaqueParameters();
        if (type != null) {
            IAType returnType = (IAType) type[0];
            return returnType;
        } else {
            return null;
        }
    }
}
