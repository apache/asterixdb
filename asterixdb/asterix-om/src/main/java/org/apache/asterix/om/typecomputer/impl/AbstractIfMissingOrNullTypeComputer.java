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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.dataflow.data.common.TypeResolverUtil;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.api.exceptions.SourceLocation;

abstract class AbstractIfMissingOrNullTypeComputer implements IResultTypeComputer {
    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        SourceLocation sourceLoc = fce.getSourceLocation();
        IAType outPrimeType = null;
        ATypeTag outQuantifier = null; // could be 'missing' or 'null'

        for (Mutable<ILogicalExpression> argRef : fce.getArguments()) {
            ILogicalExpression arg = argRef.getValue();
            IAType argType = (IAType) env.getType(arg);
            ATypeTag argTypeTag = argType.getTypeTag();

            if (equalsIfType(argTypeTag)) {
                continue;
            }

            if (argTypeTag == ATypeTag.UNION) {
                AUnionType unionType = (AUnionType) argType;
                if (intersectsIfType(unionType)) {
                    IAType primeType = getOutputPrimeType(unionType);
                    outPrimeType = outPrimeType == null ? primeType : TypeResolverUtil.resolve(outPrimeType, primeType);
                    if (outQuantifier == null) {
                        outQuantifier = getOutputQuantifier(unionType);
                    }
                } else {
                    // no intersection
                    if (outPrimeType == null) {
                        return argType;
                    } else {
                        IAType primeType = getOutputPrimeType(unionType);
                        ATypeTag quantifier = outQuantifier != null ? outQuantifier : getOutputQuantifier(unionType);
                        return createOutputType(TypeResolverUtil.resolve(outPrimeType, primeType), quantifier,
                                sourceLoc);
                    }
                }
            } else {
                // ANY or no intersection
                return outPrimeType == null ? argType
                        : createOutputType(TypeResolverUtil.resolve(outPrimeType, argType), outQuantifier, sourceLoc);
            }
        }

        if (outPrimeType == null) {
            return BuiltinType.ANULL;
        }
        IAType outType = createOutputType(outPrimeType, ATypeTag.NULL, sourceLoc);
        if (outQuantifier == ATypeTag.MISSING) {
            outType = createOutputType(outType, ATypeTag.MISSING, sourceLoc);
        }
        return outType;
    }

    protected abstract boolean equalsIfType(ATypeTag typeTag);

    protected abstract boolean intersectsIfType(AUnionType type);

    protected abstract ATypeTag getOutputQuantifier(AUnionType type);

    private IAType getOutputPrimeType(AUnionType type) {
        return type.getActualType();
    }

    private IAType createOutputType(IAType primeType, ATypeTag quantifier, SourceLocation sourceLoc)
            throws CompilationException {
        if (quantifier == null || primeType.getTypeTag() == ATypeTag.ANY) {
            return primeType;
        }
        switch (quantifier) {
            case MISSING:
                return AUnionType.createMissableType(primeType);
            case NULL:
                return AUnionType.createNullableType(primeType, null);
            default:
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc,
                        "Unexpected quantifier: " + quantifier);
        }
    }
}
