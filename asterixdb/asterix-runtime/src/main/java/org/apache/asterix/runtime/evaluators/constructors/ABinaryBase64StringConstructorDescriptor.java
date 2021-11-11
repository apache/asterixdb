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
package org.apache.asterix.runtime.evaluators.constructors;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.base.AMutableBinary;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.util.bytes.Base64Parser;

@MissingNullInOutFunction
public class ABinaryBase64StringConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = ABinaryBase64StringConstructorDescriptor::new;

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new ABinaryBase64StringConstructorEvaluator(ctx, args[0].createScalarEvaluator(ctx), sourceLoc);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.BINARY_BASE64_CONSTRUCTOR;
    }

    protected static class ABinaryBase64StringConstructorEvaluator extends AbstractBinaryConstructorEvaluator {

        private final Base64Parser parser = new Base64Parser();

        protected ABinaryBase64StringConstructorEvaluator(IEvaluatorContext ctx, IScalarEvaluator inputEval,
                SourceLocation sourceLoc) {
            super(ctx, inputEval, sourceLoc);
        }

        @Override
        protected boolean parseBinary(UTF8StringPointable textPtr, AMutableBinary result) {
            try {
                parser.generatePureByteArrayFromBase64String(textPtr.getByteArray(), textPtr.getCharStartOffset(),
                        textPtr.getUTF8Length());
            } catch (IllegalArgumentException e) {
                return false;
            }
            result.setValue(parser.getByteArray(), 0, parser.getLength());
            return true;
        }

        @Override
        protected FunctionIdentifier getIdentifier() {
            return BuiltinFunctions.BINARY_BASE64_CONSTRUCTOR;
        }
    }
}
