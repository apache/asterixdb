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

package org.apache.asterix.om.exceptions;

import java.util.function.Supplier;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.WarningUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;

public final class ExceptionUtil {

    private ExceptionUtil() {
    }

    public static String toExpectedTypeString(Object[] expectedItems) {
        StringBuilder expectedTypes = new StringBuilder();
        int numCandidateTypes = expectedItems.length;
        for (int index = 0; index < numCandidateTypes; ++index) {
            if (index > 0) {
                if (index == numCandidateTypes - 1) {
                    expectedTypes.append(" or ");
                } else {
                    expectedTypes.append(", ");
                }
            }
            expectedTypes.append(expectedItems[index]);
        }
        return expectedTypes.toString();
    }

    public static String toExpectedTypeString(byte[] expectedTypeTags) {
        StringBuilder expectedTypes = new StringBuilder();
        int numCandidateTypes = expectedTypeTags.length;
        for (int index = 0; index < numCandidateTypes; ++index) {
            if (index > 0) {
                if (index == numCandidateTypes - 1) {
                    expectedTypes.append(" or ");
                } else {
                    expectedTypes.append(", ");
                }
            }
            expectedTypes.append(EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(expectedTypeTags[index]));
        }
        return expectedTypes.toString();
    }

    public static String indexToPosition(int index) {
        int i = index + 1;
        switch (i % 100) {
            case 11:
            case 12:
            case 13:
                return i + "th";
            default:
                switch (i % 10) {
                    case 1:
                        return i + "st";
                    case 2:
                        return i + "nd";
                    case 3:
                        return i + "rd";
                    default:
                        return i + "th";
                }
        }
    }

    public static void warnTypeMismatch(IEvaluatorContext ctx, SourceLocation srcLoc, FunctionIdentifier fid,
            byte actualType, int argIdx, ATypeTag expectedType) {
        warnTypeMismatch(ctx, srcLoc, fid, actualType, argIdx, expectedType::toString);
    }

    public static void warnTypeMismatch(IEvaluatorContext ctx, SourceLocation srcLoc, FunctionIdentifier fid,
            byte actualType, int argIdx, byte[] expectedTypes) {
        warnTypeMismatch(ctx, srcLoc, fid, actualType, argIdx, () -> toExpectedTypeString(expectedTypes));
    }

    private static void warnTypeMismatch(IEvaluatorContext ctx, SourceLocation srcLoc, FunctionIdentifier fid,
            byte actualType, int argIdx, Supplier<String> expectedTypesString) {
        IWarningCollector warningCollector = ctx.getWarningCollector();
        if (warningCollector.shouldWarn()) {
            warningCollector.warn(WarningUtil.forAsterix(srcLoc, ErrorCode.TYPE_MISMATCH_FUNCTION, fid.getName(),
                    indexToPosition(argIdx), expectedTypesString.get(),
                    EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(actualType)));
        }
    }

    public static void warnIncompatibleType(IEvaluatorContext ctx, SourceLocation srcLoc, String funName,
            ATypeTag type1, ATypeTag type2) {
        IWarningCollector warningCollector = ctx.getWarningCollector();
        if (warningCollector.shouldWarn()) {
            warningCollector.warn(WarningUtil.forAsterix(srcLoc, ErrorCode.TYPE_INCOMPATIBLE, funName, type1, type2));
        }
    }

    public static void warnUnsupportedType(IEvaluatorContext ctx, SourceLocation srcLoc, String funName,
            ATypeTag unsupportedType) {
        IWarningCollector warningCollector = ctx.getWarningCollector();
        if (warningCollector.shouldWarn()) {
            warningCollector.warn(WarningUtil.forAsterix(srcLoc, ErrorCode.TYPE_UNSUPPORTED, funName, unsupportedType));
        }
    }

    /** For functions that accept an integer value (no fractions) of any numeric type including double & float */
    public static void warnNonInteger(IEvaluatorContext ctx, SourceLocation srcLoc, FunctionIdentifier fid, int argIdx,
            double argValue) {
        warnInvalidValue(ctx, srcLoc, fid, argIdx, argValue, ErrorCode.INTEGER_VALUE_EXPECTED_FUNCTION);
    }

    public static void warnNegativeValue(IEvaluatorContext ctx, SourceLocation srcLoc, FunctionIdentifier fid,
            int argIdx, double argValue) {
        warnInvalidValue(ctx, srcLoc, fid, argIdx, argValue, ErrorCode.NEGATIVE_VALUE);
    }

    private static void warnInvalidValue(IEvaluatorContext ctx, SourceLocation srcLoc, FunctionIdentifier fid,
            int argIdx, double argValue, int errorCode) {
        IWarningCollector warningCollector = ctx.getWarningCollector();
        if (warningCollector.shouldWarn()) {
            warningCollector.warn(WarningUtil.forAsterix(srcLoc, errorCode, fid.getName(), indexToPosition(argIdx),
                    Double.toString(argValue)));
        }
    }
}
