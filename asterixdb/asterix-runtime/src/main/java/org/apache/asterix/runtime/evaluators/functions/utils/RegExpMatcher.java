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

package org.apache.asterix.runtime.evaluators.functions.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.runtime.evaluators.functions.StringEvaluatorUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.data.std.util.UTF8CharSequence;

/**
 * A wrapper for regular expression processing methods.
 */
public class RegExpMatcher {

    // Library regular expression processing objects.
    private Pattern pattern = null;
    private Matcher matcher = null;

    // For storing the pattern string.
    private final ByteArrayAccessibleOutputStream lastPatternStorage = new ByteArrayAccessibleOutputStream();
    private final UTF8StringPointable lastPatternPtr = new UTF8StringPointable();

    // For storing the flag string.
    private final ByteArrayAccessibleOutputStream lastFlagsStorage = new ByteArrayAccessibleOutputStream();
    private final UTF8StringPointable lastFlagPtr = new UTF8StringPointable();

    // The char sequence for the source string.
    private final UTF8CharSequence charSeq = new UTF8CharSequence();

    //  For storing the replacement string.
    private final ByteArrayAccessibleOutputStream lastReplaceStorage = new ByteArrayAccessibleOutputStream();
    private final UTF8StringPointable lastReplaceStrPtr = new UTF8StringPointable();
    private String replaceStr = null;

    // For storing the string replacement result.
    private final StringBuffer resultBuf = new StringBuffer();

    @FunctionalInterface
    public interface IRegExpPatternGenerator {
        String toRegExpPatternString(String inputString) throws HyracksDataException;
    }

    /**
     * Builds the matcher.
     *
     * @param srcPtr
     *            , the source string for regular expression operations.
     * @param patternPtr
     *            , the definition of the regular expression.
     */
    public void build(UTF8StringPointable srcPtr, UTF8StringPointable patternPtr) throws HyracksDataException {
        build(srcPtr, patternPtr, null);
    }

    /**
     * Builds the matcher.
     *
     * @param srcPtr
     *            , the source string for regular expression operations.
     * @param patternPtr
     *            , the definition of the regular expression.
     * @param flagPtr
     *            , the flags.
     */
    public void build(UTF8StringPointable srcPtr, UTF8StringPointable patternPtr, UTF8StringPointable flagPtr)
            throws HyracksDataException {
        build(srcPtr, patternPtr, flagPtr, null);
    }

    /**
     * Builds the matcher.
     *
     * @param srcPtr
     *            , the source string for regular expression operations.
     * @param patternPtr
     *            , the definition of the regular expression.
     * @param flagPtr
     *            , the flags.
     * @param patternGenerator
     *            , the regular expression pattern generator.
     */
    public void build(UTF8StringPointable srcPtr, UTF8StringPointable patternPtr, UTF8StringPointable flagPtr,
            IRegExpPatternGenerator patternGenerator) throws HyracksDataException {
        // Builds a new pattern if necessary. patternPtr cannot be null
        final boolean newPattern = pattern == null || lastPatternPtr.compareTo(patternPtr) != 0;
        final boolean newFlag = flagPtr != null && (pattern == null || lastFlagPtr.compareTo(flagPtr) != 0);
        charSeq.reset(srcPtr);
        if (newPattern || newFlag) {
            // ! object creation !
            String patternString;
            if (newPattern) {
                StringEvaluatorUtils.copyResetUTF8Pointable(patternPtr, lastPatternStorage, lastPatternPtr);
                String inputPatternString = lastPatternPtr.toString();
                patternString = patternGenerator == null ? inputPatternString
                        : patternGenerator.toRegExpPatternString(inputPatternString);
            } else {
                // use whatever pattern string the previous pattern was using
                patternString = pattern.pattern();
            }
            int flags = 0;
            if (newFlag) {
                StringEvaluatorUtils.copyResetUTF8Pointable(flagPtr, lastFlagsStorage, lastFlagPtr);
                flags = StringEvaluatorUtils.toFlag(lastFlagPtr.toString());
            } else if (pattern != null) {
                // use whatever flags the previous pattern was using
                flags = pattern.flags();
            }
            pattern = Pattern.compile(patternString, flags);
            matcher = pattern.matcher(charSeq);
        } else {
            matcher.reset(charSeq);
        }
    }

    /**
     * Whether the source string matches the regular expression defined pattern.
     *
     * @return true if it contains the pattern; false otherwise.
     */
    public boolean matches() {
        return matcher.matches();
    }

    /**
     * Whether the source string contains the regular expression defined pattern.
     *
     * @return true if it contains the pattern; false otherwise.
     */
    public boolean find() {
        return matcher.find();
    }

    /**
     * Returns the matched string. This should be called after checking that the find()
     * method returns true.
     *
     * @return The matched string
     */
    public String group() {
        return matcher.group();
    }

    /**
     * @return the first matched position of the regular expression pattern in the source string.
     */
    public int position() {
        return matcher.find() ? matcher.start() : -1;
    }

    /**
     * Replaces the appearances of a regular expression defined pattern in a source string with a given
     * replacement string.
     *
     * @param replaceStrPtr
     *            , the string for replacing the regular expression.
     * @return a new string with contained regular expressions replaced.
     */
    public String replace(UTF8StringPointable replaceStrPtr) {
        return replace(replaceStrPtr, Integer.MAX_VALUE);
    }

    /**
     * Replaces the appearances of a regular expression defined pattern in a source string with a given
     * replacement string.
     *
     * @param replaceStrPtr
     *            , the string for replacing the regular expression.
     * @param replaceLimit
     *            , the maximum number of replacements to make
     * @return a new string with contained regular expressions replaced.
     */
    public String replace(UTF8StringPointable replaceStrPtr, int replaceLimit) {
        if (replaceLimit < 0) {
            replaceLimit = Integer.MAX_VALUE;
        }
        // Sets up a new replacement string if necessary.
        final boolean newReplace =
                replaceStrPtr != null && (replaceStr == null || lastReplaceStrPtr.compareTo(replaceStrPtr) != 0);
        if (newReplace) {
            StringEvaluatorUtils.copyResetUTF8Pointable(replaceStrPtr, lastReplaceStorage, lastReplaceStrPtr);
            replaceStr = replaceStrPtr.toString();
        }
        // Does the actual replacement.
        resultBuf.setLength(0);
        for (int i = 0; i < replaceLimit && matcher.find(); i++) {
            matcher.appendReplacement(resultBuf, replaceStr);
        }
        matcher.appendTail(resultBuf);
        return resultBuf.toString();
    }

    /**
     * Splits the provided source string using the provided regular expression.
     *
     * @return the string split tokens
     */
    public String[] split() {
        return pattern.split(charSeq);
    }
}
