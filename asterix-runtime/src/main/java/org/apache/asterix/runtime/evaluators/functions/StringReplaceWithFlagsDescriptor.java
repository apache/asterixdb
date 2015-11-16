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

public class StringReplaceWithFlagsDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new StringReplaceWithFlagsDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {

        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {

                DataOutput dout = output.getDataOutput();

                return new AbstractQuadStringStringEval(dout, args[0], args[1], args[2], args[3],
                        AsterixBuiltinFunctions.STRING_REPLACE_WITH_FLAG) {
                    private Pattern pattern = null;
                    private Matcher matcher = null;
                    private String replaceStr;
                    private String flagStr;
                    private StringBuffer resultBuf = new StringBuffer();
                    private ByteArrayAccessibleOutputStream lastPatternStorage = new ByteArrayAccessibleOutputStream();
                    private ByteArrayAccessibleOutputStream lastReplaceStorage = new ByteArrayAccessibleOutputStream();
                    private ByteArrayAccessibleOutputStream lastFlagStorage = new ByteArrayAccessibleOutputStream();
                    private UTF8StringPointable lastPatternPtr = new UTF8StringPointable();
                    private UTF8StringPointable lastReplacePtr = new UTF8StringPointable();
                    private UTF8StringPointable lastFlagPtr = new UTF8StringPointable();
                    private UTF8CharSequence carSeq = new UTF8CharSequence();

                    @Override
                    protected String compute(UTF8StringPointable srcPtr, UTF8StringPointable patternPtr,
                            UTF8StringPointable replacePtr, UTF8StringPointable flagPtr) throws AlgebricksException {
                        resultBuf.setLength(0);
                        final boolean newPattern = (pattern == null || lastPatternPtr.compareTo(patternPtr) != 0);
                        final boolean newReplace = (pattern == null || lastReplacePtr.compareTo(replacePtr) != 0);
                        final boolean newFlag = (pattern == null || lastFlagPtr.compareTo(flagPtr) != 0);

                        if (newFlag) {
                            StringEvaluatorUtils.copyResetUTF8Pointable(flagPtr, lastFlagStorage, lastFlagPtr);
                            flagStr = lastFlagPtr.toString();
                        }
                        if (newPattern) {
                            StringEvaluatorUtils.copyResetUTF8Pointable(patternPtr, lastPatternStorage, lastPatternPtr);
                        }

                        if (newPattern || newFlag) {
                            pattern = Pattern.compile(lastPatternPtr.toString(), StringEvaluatorUtils.toFlag(flagStr));
                        }

                        if (newReplace) {
                            StringEvaluatorUtils.copyResetUTF8Pointable(replacePtr, lastReplaceStorage, lastReplacePtr);
                            replaceStr = replacePtr.toString();
                        }

                        carSeq.reset(srcPtr);
                        if (newPattern || newFlag) {
                            matcher = pattern.matcher(carSeq);
                        } else {
                            matcher.reset(carSeq);
                        }

                        while (matcher.find()) {
                            matcher.appendReplacement(resultBuf, replaceStr);
                        }
                        matcher.appendTail(resultBuf);
                        return resultBuf.toString();
                    }

                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.STRING_REPLACE_WITH_FLAG;
    }
}
