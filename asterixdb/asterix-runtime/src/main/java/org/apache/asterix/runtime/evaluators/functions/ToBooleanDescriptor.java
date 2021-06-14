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
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.constructors.AbstractBooleanConstructorEvaluator;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.util.string.UTF8StringUtil;

@MissingNullInOutFunction
public class ToBooleanDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = ToBooleanDescriptor::new;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractBooleanConstructorEvaluator(ctx, args[0].createScalarEvaluator(ctx), sourceLoc) {
                    @Override
                    protected void handleUnsupportedType(ATypeTag inputType, IPointable result)
                            throws HyracksDataException {
                        byte[] bytes = inputArg.getByteArray();
                        int startOffset = inputArg.getStartOffset();
                        int len = inputArg.getLength();
                        switch (inputType) {
                            case ARRAY:
                                int n = AOrderedListSerializerDeserializer.getNumberOfItems(bytes, startOffset);
                                setInteger(n, result);
                                break;
                            case MULTISET:
                                n = AUnorderedListSerializerDeserializer.getNumberOfItems(bytes, startOffset);
                                setInteger(n, result);
                                break;
                            case OBJECT:
                                boolean empty = ARecordSerializerDeserializer.hasNoFields(bytes, startOffset, len);
                                setBoolean(result, !empty);
                                break;
                            default:
                                super.handleUnsupportedType(inputType, result);
                                break;
                        }
                    }

                    @Override
                    protected Boolean parseBoolean(byte[] bytes, int offset, int len) {
                        return UTF8StringUtil.getStringLength(bytes, offset + 1) != 0;
                    }

                    @Override
                    protected FunctionIdentifier getIdentifier() {
                        return ToBooleanDescriptor.this.getIdentifier();
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.TO_BOOLEAN;
    }
}
