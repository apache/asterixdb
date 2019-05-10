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

import java.util.List;

import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class TypeComputeUtils {

    private static final byte CERTAIN = 1;
    private static final byte NULLABLE = 2;
    private static final byte MISSABLE = 4;
    private static final byte MISSING = 8;
    private static final byte NULL = 16;

    @FunctionalInterface
    public interface ArgTypeChecker {
        void checkArgTypes(int argIndex, IAType argType, SourceLocation argSrcLoc) throws AlgebricksException;
    }

    @FunctionalInterface
    public interface ResultTypeGenerator {
        IAType getResultType(ILogicalExpression expr, IAType... knownInputTypes) throws AlgebricksException;
    }

    private TypeComputeUtils() {
    }

    /**
     * Resolve the result type of an expression.
     *
     * @param expr,
     *            the expression to consider.
     * @param env,
     *            the type environment.
     * @param checker,
     *            the argument type checker.
     * @param resultTypeGenerator,
     *            the result type generator.
     * @param propagateNullAndMissing,
     *            whether the expression follows MISSING/NULL-in-MISSING/NULL-out semantics.
     * @return the resolved result type with considering optional types.
     * @throws AlgebricksException
     */
    public static IAType resolveResultType(ILogicalExpression expr, IVariableTypeEnvironment env,
            ArgTypeChecker checker, ResultTypeGenerator resultTypeGenerator, boolean propagateNullAndMissing)
            throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expr;

        List<Mutable<ILogicalExpression>> arguments = fce.getArguments();
        IAType[] inputTypes = new IAType[arguments.size()];
        int index = 0;
        for (Mutable<ILogicalExpression> argRef : arguments) {
            ILogicalExpression arg = argRef.getValue();
            inputTypes[index++] = (IAType) env.getType(arg);
        }

        // Checks input types.
        IAType[] knownInputTypes = TypeComputeUtils.getActualType(inputTypes);
        boolean[] unknownable = TypeComputeUtils.isUnknownableType(inputTypes);
        for (int argIndex = 0; argIndex < knownInputTypes.length; ++argIndex) {
            ATypeTag argTypeTag = knownInputTypes[argIndex].getTypeTag();
            if (unknownable[argIndex] || argTypeTag == ATypeTag.ANY || argTypeTag == ATypeTag.NULL
                    || argTypeTag == ATypeTag.MISSING) {
                continue;
            }
            checker.checkArgTypes(argIndex, knownInputTypes[argIndex], fce.getSourceLocation());
        }

        // Computes the result type.
        byte category = TypeComputeUtils.resolveCategory(inputTypes);
        if (propagateNullAndMissing) {
            if (category == MISSING) {
                return BuiltinType.AMISSING;
            }
            if (category == NULL) {
                return BuiltinType.ANULL;
            }
            boolean metNull = false;
            for (IAType knownInputType : knownInputTypes) {
                ATypeTag typeTag = knownInputType.getTypeTag();
                // Returns missing if there is one missing.
                if (typeTag == ATypeTag.MISSING) {
                    return knownInputType;
                }
                if (typeTag == ATypeTag.NULL) {
                    metNull = true;
                }
            }
            // Returns null if there is one null but there is no missing.
            if (metNull) {
                return BuiltinType.ANULL;
            }
            return TypeComputeUtils.getResultType(resultTypeGenerator.getResultType(expr, knownInputTypes), category);
        } else {
            return resultTypeGenerator.getResultType(expr, knownInputTypes);
        }
    }

    private static byte resolveCategory(IAType... inputTypes) {
        byte category = CERTAIN;
        boolean meetNull = false;
        for (IAType inputType : inputTypes) {
            switch (inputType.getTypeTag()) {
                case UNION:
                    AUnionType unionType = (AUnionType) inputType;
                    if (unionType.isNullableType()) {
                        category |= NULLABLE;
                    }
                    if (unionType.isMissableType()) {
                        category |= MISSABLE;
                    }
                    break;
                case MISSING:
                    return MISSING;
                case NULL:
                    meetNull = true;
                    break;
                case ANY:
                    category |= NULLABLE;
                    category |= MISSABLE;
                    break;
                default:
                    break;
            }
        }
        if (meetNull) {
            return NULL;
        }
        return category;
    }

    private static IAType getResultType(IAType type, byte category) {
        if (type.getTypeTag() == ATypeTag.ANY) {
            return type;
        }
        if (category == CERTAIN) {
            return type;
        }
        IAType resultType = type;
        if ((category & NULLABLE) != 0 || (category & NULL) != 0) {
            resultType = AUnionType.createNullableType(resultType);
        }
        if ((category & MISSABLE) != 0 || (category & MISSING) != 0) {
            resultType = AUnionType.createMissableType(resultType);
        }
        return resultType;
    }

    private static IAType[] getActualType(IAType... inputTypes) {
        IAType[] actualTypes = new IAType[inputTypes.length];
        int index = 0;
        for (IAType inputType : inputTypes) {
            actualTypes[index++] = getActualType(inputType);
        }
        return actualTypes;
    }

    private static boolean[] isUnknownableType(IAType... inputTypes) {
        boolean[] unknownable = new boolean[inputTypes.length];
        for (int index = 0; index < unknownable.length; ++index) {
            IAType type = inputTypes[index];
            unknownable[index] = false;
            if (type.getTypeTag() != ATypeTag.UNION) {
                continue;
            } else {
                AUnionType unionType = (AUnionType) type;
                unknownable[index] = unionType.isUnknownableType();
            }
        }
        return unknownable;
    }

    public static IAType getActualType(IAType inputType) {
        return inputType.getTypeTag() == ATypeTag.UNION ? ((AUnionType) inputType).getActualType() : inputType;
    }

    public static ARecordType extractRecordType(IAType t) {
        switch (t.getTypeTag()) {
            case OBJECT:
                return (ARecordType) t;
            case UNION:
                IAType innerType = ((AUnionType) t).getActualType();
                if (innerType.getTypeTag() == ATypeTag.OBJECT) {
                    return (ARecordType) innerType;
                } else {
                    return null;
                }
            case ANY:
                return RecordUtil.FULLY_OPEN_RECORD_TYPE;
            default:
                return null;
        }
    }

    public static AOrderedListType extractOrderedListType(IAType t) {
        if (t.getTypeTag() == ATypeTag.ARRAY) {
            return (AOrderedListType) t;
        }

        if (t.getTypeTag() == ATypeTag.UNION) {
            IAType innerType = ((AUnionType) t).getActualType();
            if (innerType.getTypeTag() == ATypeTag.ARRAY) {
                return (AOrderedListType) innerType;
            }
        }
        return null;
    }

    public static IAType extractListItemType(IAType t) {
        IAType primeType = getActualType(t);
        ATypeTag primeTypeTag = primeType.getTypeTag();
        if (primeTypeTag.isListType()) {
            return ((AbstractCollectionType) primeType).getItemType();
        } else if (primeTypeTag == ATypeTag.ANY) {
            return primeType;
        } else {
            return null;
        }
    }

    // this is for complex types. it will return null when asking for a default type for a primitive tag
    public static IAType getActualTypeOrOpen(IAType type, ATypeTag tag) {
        IAType actualType = TypeComputeUtils.getActualType(type);
        return actualType.getTypeTag() == ATypeTag.ANY ? DefaultOpenFieldType.getDefaultOpenFieldType(tag) : actualType;
    }
}
