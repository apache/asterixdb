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

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * This function takes 2 arguments, first argument is a list of strings, and second argument is a string separator,
 * the function concatenates the strings in the first list argument together and returns that as a result.
 *
 * If only a single string is passed in the list, the separator is not added to the result.
 *
 * The behavior is as follows:
 * - If the first argument is a list of strings, and the second argument is a string, concatenated string is returned.
 * - If any argument is missing, missing is returned.
 * - If any argument is null, null is returned.
 * - If any item is not a string, or the array is containing non-strings, null is returned.
 *
 * Examples:
 * string_join(["1", "2"], "-") -> "1-2"
 * string_join(["1"], "-") -> "1"
 */
@MissingNullInOutFunction
public class StringJoinDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = StringJoinDescriptor::new;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractConcatStringEval(args, ctx, sourceLoc, getIdentifier(), 1) {

                    // TODO Need a way to report array of strings is the expected type
                    private final byte[] ARRAY_TYPE = new byte[] { ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG };

                    @Override
                    protected boolean isAcceptedType(ATypeTag typeTag) {
                        return typeTag.isListType();
                    }

                    @Override
                    protected byte[] getExpectedType() {
                        return ARRAY_TYPE;
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.STRING_JOIN;
    }
}