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

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.AbstractMultiTypeCheckEvaluator;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Validates that the input is either an ordered or unordered list.
 * <p>This check is specifically used in nested {@code UPDATE} statements, where the value
 * being updated in the inner update must also be a list.
 * <p><b>Example:</b>
 * <pre>
 * UPDATE Orders AS o
 *   (UPDATE o.b1 AS o1
 *    SET o1.v1 = 1);
 * </pre>
 * <p>In this case, {@code b1} should be a list. If it is an empty list, the return value
 * from this function will still include the empty list.
 * <p>If the input is not an ordered or unordered list, a {@code DataTransformException}
 * is thrown. Otherwise, the input is returned unchanged.
 */
public class CheckListDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = CheckListDescriptor::new;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractMultiTypeCheckEvaluator(args[0].createScalarEvaluator(ctx),
                        ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG, ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG) {

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        eval.evaluate(tuple, argPtr);
                        byte typeTag = argPtr.getByteArray()[argPtr.getStartOffset()];
                        if (isMatch(typeTag)) {
                            result.set(argPtr);
                        } else {
                            if (ctx.getWarningCollector().shouldWarn()) {
                                ctx.getWarningCollector().warn(Warning.of(sourceLoc, ErrorCode.TYPE_UNSUPPORTED,
                                        getIdentifier(), EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(typeTag)));
                            }
                            throw new RuntimeDataException(ErrorCode.UPDATE_TARGET_NOT_LIST,
                                    EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(typeTag).toString());
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.CHECK_LIST;
    }

}
