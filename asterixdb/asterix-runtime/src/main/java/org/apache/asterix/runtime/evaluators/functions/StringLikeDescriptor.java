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

import static org.apache.asterix.runtime.evaluators.functions.StringEvaluatorUtils.RESERVED_REGEX_CHARS;

import java.util.Arrays;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.utils.RegExpMatcher;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

/**
 * Runtime for {@code LIKE} operator. The invocation is LIKE(test_string, pattern). Pattern of LIKE includes two
 * wildcards, "%" and "_". The "%" matches zero or more characters. The "_" matches any single character. Line
 * terminator characters are included, e.g. "\n", "\r\n", etc.
 * <p>
 * The default escape character is a backslash. Currently, the default cannot be changed, but will be configurable in
 * the future. The escape character is used to match literal "%", "_" and the escape character itself. It is an error
 * to use the escape character to match a character other than those three. It is also an error if the pattern ends
 * with an incomplete escape character sequence since it not known what character the user wants to match literally.
 * <p>
 * Backslash character is written as "\\" inside a string. That is how a backslash character is supposed to be
 * written inside a string. That means if the escape character is backslash and the goal is to match a literal "%",
 * write "\\%" in the pattern string. To match a literal "_" write "\\_". To match the backslash (the escape character)
 * write "\\\\". The first "\\" is the LIKE escape character. The last "\\" is the backslash character to match.
 * <p>
 * Examples using the default escape character, the backslash:
 * <ul>
 *     <li>LIKE("there is a 50% discount", "%50\\% discount") -> true</li>
 *     <li>LIKE("text_text", "%\\_%") -> true</li>
 *     <li>LIKE("text with a backslash \\ in here", "%\\\\%") -> true</li>
 *     <li>LIKE("text with a backslash \\", "%\\") -> ERROR</li>
 *     <li>LIKE("Foo and bar text", "%\\ext") -> ERROR</li>
 * </ul>
 * <p>
 * Creates new Matcher and Pattern objects each time the value of the pattern argument (the second argument) changes.
 */

@MissingNullInOutFunction
public class StringLikeDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = StringLikeDescriptor::new;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.STRING_LIKE;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new StringLikeEval(ctx, args[0], args[1], StringLikeDescriptor.this.getIdentifier(), sourceLoc);
            }
        };
    }

    private static class StringLikeEval extends AbstractBinaryStringBoolEval
            implements RegExpMatcher.IRegExpPatternGenerator {

        // could be improved to check if args are constant and create a matcher with fixed pattern/flags
        private static final UTF8StringPointable DOT_ALL_FLAG = UTF8StringPointable.generateUTF8Pointable("s");
        private static final char likeEscapeChar = '\\'; // currently static until ESCAPE is supported
        private final RegExpMatcher matcher = new RegExpMatcher();
        private final StringBuilder tempStringBuilder = new StringBuilder();

        StringLikeEval(IEvaluatorContext context, IScalarEvaluatorFactory evalLeftFactory,
                IScalarEvaluatorFactory evalRightFactory, FunctionIdentifier funcID, SourceLocation sourceLoc)
                throws HyracksDataException {
            super(context, evalLeftFactory, evalRightFactory, funcID, sourceLoc);
        }

        @Override
        protected boolean compute(UTF8StringPointable srcPtr, UTF8StringPointable patternPtr)
                throws HyracksDataException {
            matcher.build(srcPtr, patternPtr, DOT_ALL_FLAG, this);
            return matcher.matches();
        }

        @Override
        public String toRegExpPatternString(String pattern) throws HyracksDataException {
            tempStringBuilder.setLength(0);
            for (int i = 0, length = pattern.length(); i < length; i++) {
                char c = pattern.charAt(i);
                if (c == likeEscapeChar) {
                    char nextChar;
                    // escape character can't be last, and only %, _ and the escape char are allowed after it
                    if (i >= length - 1 || ((nextChar = pattern.charAt(i + 1)) != '%' && nextChar != '_'
                            && nextChar != likeEscapeChar)) {
                        throw new RuntimeDataException(ErrorCode.INVALID_LIKE_PATTERN, this.sourceLoc, pattern);
                    }
                    if (Arrays.binarySearch(RESERVED_REGEX_CHARS, nextChar) >= 0) {
                        // precede the nextChar with a backslash if it's one of JAVA's regex reserved chars
                        tempStringBuilder.append('\\');
                    }
                    tempStringBuilder.append(nextChar);
                    ++i;
                } else if (c == '%') {
                    tempStringBuilder.append(".*");
                } else if (c == '_') {
                    tempStringBuilder.append('.');
                } else {
                    if (Arrays.binarySearch(RESERVED_REGEX_CHARS, c) >= 0) {
                        tempStringBuilder.append('\\');
                    }
                    tempStringBuilder.append(c);
                }
            }
            return tempStringBuilder.toString();
        }
    }
}
