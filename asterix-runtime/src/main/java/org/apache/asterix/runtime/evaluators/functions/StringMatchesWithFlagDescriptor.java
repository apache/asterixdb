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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.asterix.runtime.evaluators.functions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.data.std.util.UTF8CharSequence;

public class StringMatchesWithFlagDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new StringMatchesWithFlagDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {

        return new IScalarEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IHyracksTaskContext ctx) throws AlgebricksException {

                return new AbstractTripleStringBoolEval(ctx, args[0], args[1], args[2],
                        AsterixBuiltinFunctions.STRING_MATCHES_WITH_FLAG) {
                    private Pattern pattern = null;
                    private Matcher matcher = null;
                    private ByteArrayAccessibleOutputStream lastPatternStorage = new ByteArrayAccessibleOutputStream();
                    private ByteArrayAccessibleOutputStream lastFlagsStorage = new ByteArrayAccessibleOutputStream();
                    private UTF8StringPointable lastPatternPtr = new UTF8StringPointable();
                    private UTF8StringPointable lastFlagPtr = new UTF8StringPointable();
                    private UTF8CharSequence carSeq = new UTF8CharSequence();

                    @Override
                    protected boolean compute(UTF8StringPointable strSrc, UTF8StringPointable strPattern,
                            UTF8StringPointable strFlag) throws AlgebricksException {
                        final boolean newPattern = (pattern == null || lastPatternPtr.compareTo(strPattern) != 0);
                        final boolean newFlag = (pattern == null || lastFlagPtr.compareTo(strFlag) != 0);

                        if (newPattern) {
                            StringEvaluatorUtils.copyResetUTF8Pointable(strPattern, lastPatternStorage, lastPatternPtr);
                        }

                        if (newFlag) {
                            StringEvaluatorUtils.copyResetUTF8Pointable(strFlag, lastFlagsStorage, lastFlagPtr);
                        }

                        if (newPattern || newFlag) {
                            pattern = Pattern.compile(lastPatternPtr.toString(),
                                    StringEvaluatorUtils.toFlag(lastFlagPtr.toString()));
                        }
                        carSeq.reset(strSrc);
                        if (newPattern || newFlag) {
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

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.STRING_MATCHES_WITH_FLAG;
    }
}
