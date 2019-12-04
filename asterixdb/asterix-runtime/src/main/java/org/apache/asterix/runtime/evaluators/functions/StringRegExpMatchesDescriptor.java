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

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.utils.RegExpMatcher;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;

/**
 * This function takes 2 arguments, a string, and a pattern
 */
@MissingNullInOutFunction
public class StringRegExpMatchesDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = StringRegExpMatchesDescriptor::new;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractBinaryStringEval(ctx, args[0], args[1], getIdentifier(), sourceLoc) {
                    private final RegExpMatcher matcher = new RegExpMatcher();

                    private final UTF8StringBuilder stringBuilder = new UTF8StringBuilder();
                    private final GrowableArray stringBuilderArray = new GrowableArray();

                    private final IAsterixListBuilder listBuilder = new OrderedListBuilder();
                    private final AbstractCollectionType collectionType =
                            new AOrderedListType(BuiltinType.ASTRING, BuiltinType.ASTRING.getTypeName());

                    @Override
                    protected void process(UTF8StringPointable srcPtr, UTF8StringPointable patternPtr,
                            IPointable result) throws HyracksDataException {
                        matcher.build(srcPtr, patternPtr);

                        // Result is a list of type strings
                        listBuilder.reset(collectionType);

                        try {
                            // Add all the matches to the builder
                            while (matcher.find()) {
                                String match = matcher.group();
                                stringBuilderArray.reset();

                                // Estimated length is number of characters + 1 (1 byte for string length)
                                stringBuilder.reset(stringBuilderArray, match.length() + 1);
                                stringBuilder.appendString(match);
                                stringBuilder.finish();

                                resultStorage.reset();
                                dataOutput.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                                dataOutput.write(stringBuilderArray.getByteArray(), 0, stringBuilderArray.getLength());
                                listBuilder.addItem(resultStorage);
                            }

                            resultStorage.reset();
                            listBuilder.write(dataOutput, true);
                            result.set(resultStorage);
                        } catch (IOException ex) {
                            throw HyracksDataException.create(ex);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.STRING_REGEXP_MATCHES;
    }
}
