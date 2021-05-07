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

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

@MissingNullInOutFunction
public class DecodeDataverseDisplayNameDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = DecodeDataverseDisplayNameDescriptor::new;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractUnaryStringStringEval(ctx, args[0], getIdentifier(), sourceLoc) {

                    private final StringBuilder sb = new StringBuilder();

                    @Override
                    void process(UTF8StringPointable inputString, IPointable resultPointable) throws IOException {
                        String dataverseCanonicalName = inputString.toString();

                        sb.setLength(0);
                        try {
                            DataverseName.getDisplayFormFromCanonicalForm(dataverseCanonicalName, sb);
                        } catch (AsterixException e) {
                            return; // writeResult() will emit NULL
                        }

                        resultBuilder.reset(resultArray, inputString.getUTF8Length());
                        resultBuilder.appendString(sb);
                        resultBuilder.finish();
                    }

                    @Override
                    void writeResult(IPointable resultPointable) throws IOException {
                        if (sb.length() == 0) {
                            PointableHelper.setNull(resultPointable);
                        } else {
                            super.writeResult(resultPointable);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.DECODE_DATAVERSE_DISPLAY_NAME;
    }
}
