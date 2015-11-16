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

import java.io.DataOutput;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.data.std.util.UTF8CharSequence;

/**
 * Creates new Matcher and Pattern objects each time the value of the pattern
 * argument (the second argument) changes.
 */

public class StringLikeDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new StringLikeDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.STRING_LIKE;
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {

        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {

                final DataOutput dout = output.getDataOutput();

                return new AbstractBinaryStringBoolEval(dout, args[0], args[1],
                        AsterixBuiltinFunctions.STRING_MATCHES) {

                    private Pattern pattern = null;
                    private Matcher matcher = null;
                    private ByteArrayAccessibleOutputStream lastPatternStorage = new ByteArrayAccessibleOutputStream();
                    private UTF8StringPointable lastPatternPtr = new UTF8StringPointable();
                    private UTF8CharSequence carSeq = new UTF8CharSequence();

                    @Override
                    protected boolean compute(UTF8StringPointable srcPtr, UTF8StringPointable patternPtr)
                            throws AlgebricksException {
                        boolean newPattern = false;
                        if (pattern == null || lastPatternPtr.compareTo(patternPtr) != 0) {
                            newPattern = true;
                        }
                        if (newPattern) {
                            StringEvaluatorUtils.copyResetUTF8Pointable(patternPtr, lastPatternStorage, lastPatternPtr);
                            // ! object creation !
                            pattern = Pattern.compile(StringEvaluatorUtils.toRegex(lastPatternPtr.toString()));
                        }

                        carSeq.reset(srcPtr);
                        if (newPattern) {
                            matcher = pattern.matcher(carSeq);
                        } else {
                            matcher.reset(carSeq);
                        }
                        return matcher.find();
                    }

                };
            }
        };
    }

};

