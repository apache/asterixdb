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

import java.io.IOException;

import org.apache.asterix.runtime.evaluators.functions.StringEvaluatorUtils;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;

import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;

/**
 * A wrapper for string trim methods.
 */
public class StringTrimmer {

    // For the char set to trim.
    private final ByteArrayAccessibleOutputStream lastPatternStorage = new ByteArrayAccessibleOutputStream();
    private final UTF8StringPointable lastPatternPtr = new UTF8StringPointable();
    private IntSet codePointSet = new IntArraySet();

    // For outputting the result.
    private final UTF8StringBuilder resultBuilder;
    private final GrowableArray resultArray;

    /**
     * @param resultBuilder
     *            , the builder for result strings.
     * @param resultArray
     *            , the byte array to hold results.
     */
    public StringTrimmer(UTF8StringBuilder resultBuilder, GrowableArray resultArray) {
        this(resultBuilder, resultArray, null);
    }

    /**
     * @param resultBuilder
     *            , the builder for result strings.
     * @param resultArray
     *            , the byte array to hold results.
     * @param pattern
     *            , the string that is used to construct the charset for trimming.
     */
    public StringTrimmer(UTF8StringBuilder resultBuilder, GrowableArray resultArray, UTF8StringPointable pattern) {
        this.resultBuilder = resultBuilder;
        this.resultArray = resultArray;
        if (pattern != null) {
            codePointSet.clear();
            pattern.getCodePoints(codePointSet);
        }
    }

    /**
     * Builds the charset from a pattern string.
     *
     * @param patternPtr
     *            , a pattern string.
     */
    public void build(UTF8StringPointable patternPtr) {
        final boolean newPattern = (codePointSet.size() == 0) || lastPatternPtr.compareTo(patternPtr) != 0;
        if (newPattern) {
            StringEvaluatorUtils.copyResetUTF8Pointable(patternPtr, lastPatternStorage, lastPatternPtr);
            codePointSet.clear();
            patternPtr.getCodePoints(codePointSet);
        }
    }

    /**
     * Trims an input source string and lets <code>resultStrPtr</code> points to the resulting string.
     *
     * @param srcPtr
     *            , an input source string.
     * @param resultStrPtr
     *            , a pointable that is supposed to point to the resulting string.
     * @param left
     *            , whether to trim the left side.
     * @param right
     *            , whether to trim the right side.
     * @throws IOException
     */
    public void trim(UTF8StringPointable srcPtr, IPointable resultStrPtr, boolean left, boolean right)
            throws IOException {
        srcPtr.trim(resultBuilder, resultArray, left, right, codePointSet);
        resultStrPtr.set(resultArray.getByteArray(), 0, resultArray.getLength());
    }

}
