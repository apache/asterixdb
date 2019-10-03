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

package org.apache.asterix.runtime.evaluators.common;

import static org.apache.asterix.om.types.ATypeTag.VALUE_TYPE_MAPPING;

import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.types.hierachy.DoubleToInt32TypeConvertComputer;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

import com.google.common.math.DoubleMath;

/**
 * Utility methods for argument handling
 */
public final class ArgumentUtils {

    public static final byte[] EXPECTED_NUMERIC = { ATypeTag.SERIALIZED_INT8_TYPE_TAG,
            ATypeTag.SERIALIZED_INT16_TYPE_TAG, ATypeTag.SERIALIZED_INT32_TYPE_TAG, ATypeTag.SERIALIZED_INT64_TYPE_TAG,
            ATypeTag.SERIALIZED_FLOAT_TYPE_TAG, ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG };
    private static final DoubleToInt32TypeConvertComputer LAX_DOUBLE_TO_INT32 =
            DoubleToInt32TypeConvertComputer.getInstance(false);

    private ArgumentUtils() {
    }

    /**
     * Checks that {@code value} is of numeric type and a finite mathematical integer (i.e. no fractions) returning
     * true if it is and storing the integer value in {@code outInteger}. Otherwise, returns false and issues a
     * warning. If the integer value of {@code value} cannot fit in an int32, then {@link Integer#MAX_VALUE} or
     * {@link Integer#MIN_VALUE} is stored.
     *
     * @param value data to be checked
     * @param outInteger where the integer read from {@code value} will be stored
     */
    public static boolean checkWarnOrSetInteger(IEvaluatorContext ctx, SourceLocation sourceLoc,
            FunctionIdentifier funcID, int argIdx, byte[] value, int offset, AMutableInt32 outInteger)
            throws HyracksDataException {
        byte type = value[offset];
        if (ATypeHierarchy.getTypeDomain(VALUE_TYPE_MAPPING[type]) != ATypeHierarchy.Domain.NUMERIC) {
            ExceptionUtil.warnTypeMismatch(ctx, sourceLoc, funcID, type, argIdx, EXPECTED_NUMERIC);
            return false;
        }
        // deal with NaN, +/-INF
        double doubleValue = ATypeHierarchy.getDoubleValue(funcID.getName(), argIdx, value, offset);
        if (!DoubleMath.isMathematicalInteger(doubleValue)) {
            ExceptionUtil.warnNonInteger(ctx, sourceLoc, funcID, argIdx, doubleValue);
            return false;
        }
        outInteger.setValue(LAX_DOUBLE_TO_INT32.convert(doubleValue));
        return true;
    }
}
