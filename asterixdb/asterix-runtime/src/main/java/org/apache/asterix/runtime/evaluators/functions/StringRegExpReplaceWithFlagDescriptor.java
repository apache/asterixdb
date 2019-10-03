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
package org.apache.asterix.runtime.evaluators.functions;

import java.io.IOException;
import java.util.Arrays;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ArgumentUtils;
import org.apache.asterix.runtime.evaluators.functions.utils.RegExpMatcher;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

@MissingNullInOutFunction
public class StringRegExpReplaceWithFlagDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = StringRegExpReplaceWithFlagDescriptor::new;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractQuadStringStringEval(ctx, args[0], args[1], args[2], args[3],
                        StringRegExpReplaceWithFlagDescriptor.this.getIdentifier(), sourceLoc) {
                    private final UTF8StringPointable emptyFlags = UTF8StringPointable.generateUTF8Pointable("");
                    private final RegExpMatcher matcher = new RegExpMatcher();
                    private final AMutableInt32 mutableInt = new AMutableInt32(0);
                    private byte[] expectedTypes;
                    private int limit;

                    @Override
                    protected boolean processArgument(int argIdx, IPointable argPtr, UTF8StringPointable outStrPtr)
                            throws HyracksDataException {
                        if (argIdx == 3) {
                            byte[] bytes = argPtr.getByteArray();
                            int start = argPtr.getStartOffset();
                            ATypeTag tt = ATypeTag.VALUE_TYPE_MAPPING[bytes[start]];
                            if (ATypeHierarchy.getTypeDomain(tt) != ATypeHierarchy.Domain.NUMERIC
                                    && tt != ATypeTag.STRING) {
                                ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, funcID, bytes[start], argIdx,
                                        getExpectedTypes());
                                return false;
                            }
                            switch (tt) {
                                case TINYINT:
                                case SMALLINT:
                                case INTEGER:
                                case BIGINT:
                                case FLOAT:
                                case DOUBLE:
                                    if (!ArgumentUtils.checkWarnOrSetInteger(ctx, sourceLoc, funcID, argIdx, bytes,
                                            start, mutableInt)) {
                                        return false;
                                    }
                                    limit = mutableInt.getIntegerValue();
                                    outStrPtr.set(emptyFlags);
                                    return true;
                                default:
                                    limit = Integer.MAX_VALUE;
                                    break;
                            }
                        }
                        return super.processArgument(argIdx, argPtr, outStrPtr);
                    }

                    @Override
                    protected String compute(UTF8StringPointable srcPtr, UTF8StringPointable patternPtr,
                            UTF8StringPointable replacePtr, UTF8StringPointable flagsPtr) throws IOException {
                        matcher.build(srcPtr, patternPtr, flagsPtr);
                        return matcher.replace(replacePtr, limit);
                    }

                    private byte[] getExpectedTypes() {
                        if (expectedTypes == null) {
                            expectedTypes = Arrays.copyOf(ArgumentUtils.EXPECTED_NUMERIC,
                                    ArgumentUtils.EXPECTED_NUMERIC.length + 1);
                            expectedTypes[expectedTypes.length - 1] = ATypeTag.SERIALIZED_STRING_TYPE_TAG;
                        }
                        return expectedTypes;
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.STRING_REGEXP_REPLACE_WITH_FLAG;
    }
}
