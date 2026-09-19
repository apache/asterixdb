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
package org.apache.asterix.optimizer.rules.util;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.base.ADate;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TimeTravel;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Reads the constant an {@code AT SNAPSHOT} / {@code AT TIMESTAMP} value folded down to, and renders it in one
 * of the forms the Iceberg reader parses.
 * <p>
 * The value rides the ordinary expression machinery: it is an expression reference on the unnest operator, so
 * constant folding reduces it during normalization -- before {@code UnnestToDataScanRule} turns the unnest into
 * the scan that consumes it. What is left here is only reading the result and rejecting what could not be used:
 * the snapshot selects which manifest and data files the scan reads, so there is no runtime evaluation path for
 * it and it has to be a constant by the time the scan is built.
 *
 * @see org.apache.asterix.external.util.iceberg.IcebergSnapshotUtils
 */
public final class TimeTravelUtil {

    private static final String EXPECTED_FOR_SNAPSHOT_ID = "an integer or a string";

    private static final String EXPECTED_FOR_SNAPSHOT_TIMESTAMP = "an integer, a string, a date or a datetime";

    /**
     * One of the forms {@code IcebergSnapshotUtils} accepts: always seconds, fractional part only when there is
     * one. {@link DateTimeFormatter#ISO_LOCAL_DATE_TIME} would drop {@code :00} seconds, which still parses but
     * makes the value look truncated.
     */
    private static final DateTimeFormatter SNAPSHOT_TIMESTAMP_FORMAT =
            new DateTimeFormatterBuilder().append(ISO_LOCAL_DATE).appendLiteral('T').appendValue(HOUR_OF_DAY, 2)
                    .appendLiteral(':').appendValue(MINUTE_OF_HOUR, 2).appendLiteral(':')
                    .appendValue(SECOND_OF_MINUTE, 2).appendFraction(NANO_OF_SECOND, 0, 3, true).toFormatter();

    private TimeTravelUtil() {
    }

    /**
     * @param timeTravel the specification carried by the unnest operator
     * @param sourceLoc where to report a failure against
     * @return the value in the string form the Iceberg reader parses
     * @throws CompilationException if the value did not fold to a constant, or folded to something unusable
     */
    public static String resolve(TimeTravel timeTravel, SourceLocation sourceLoc) throws CompilationException {
        ILogicalExpression valueExpr = timeTravel.getValueExpression();
        if (valueExpr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            throw new CompilationException(ErrorCode.EXPECTED_CONSTANT_VALUE, sourceLoc);
        }
        IAlgebricksConstantValue constantValue = ((ConstantExpression) valueExpr).getValue();
        if (constantValue.isNull() || constantValue.isMissing()) {
            // e.g. 'date("2026-08-20") - 1': date arithmetic only accepts durations, and a type mismatch
            // inside an arithmetic function produces null rather than an error.
            throw invalidValue(timeTravel.getType(), sourceLoc, constantValue.isNull() ? "null" : "missing");
        }
        if (!(constantValue instanceof AsterixConstantValue)) {
            throw new CompilationException(ErrorCode.EXPECTED_CONSTANT_VALUE, sourceLoc);
        }
        return stringify(((AsterixConstantValue) constantValue).getObject(), timeTravel.getType(), sourceLoc);
    }

    public static String stringify(IAObject value, TimeTravel.Type type, SourceLocation sourceLoc)
            throws CompilationException {
        ATypeTag typeTag = value.getType().getTypeTag();
        switch (typeTag) {
            case TINYINT:
                return String.valueOf(((AInt8) value).getByteValue());
            case SMALLINT:
                return String.valueOf(((AInt16) value).getShortValue());
            case INTEGER:
                return String.valueOf(((AInt32) value).getIntegerValue());
            case BIGINT:
                return String.valueOf(((AInt64) value).getLongValue());
            case STRING:
                return ((AString) value).getStringValue();
            case DATE:
                requireTimestamp(type, sourceLoc, typeTag);
                return LocalDate.ofEpochDay(((ADate) value).getChrononTimeInDays()).format(ISO_LOCAL_DATE);
            case DATETIME:
                requireTimestamp(type, sourceLoc, typeTag);
                return LocalDateTime
                        .ofInstant(Instant.ofEpochMilli(((ADateTime) value).getChrononTime()), ZoneOffset.UTC)
                        .format(SNAPSHOT_TIMESTAMP_FORMAT);
            default:
                throw invalidValue(type, sourceLoc, typeTag.toString().toLowerCase());
        }
    }

    /**
     * A date or datetime only makes sense for {@code AT TIMESTAMP}; a snapshot id is an integer.
     */
    private static void requireTimestamp(TimeTravel.Type type, SourceLocation sourceLoc, ATypeTag typeTag)
            throws CompilationException {
        if (type != TimeTravel.Type.SNAPSHOT_TIMESTAMP) {
            throw invalidValue(type, sourceLoc, typeTag.toString().toLowerCase());
        }
    }

    private static CompilationException invalidValue(TimeTravel.Type type, SourceLocation sourceLoc, String actual) {
        String expected =
                type == TimeTravel.Type.SNAPSHOT_TIMESTAMP ? EXPECTED_FOR_SNAPSHOT_TIMESTAMP : EXPECTED_FOR_SNAPSHOT_ID;
        return new CompilationException(ErrorCode.INVALID_TIME_TRAVEL_VALUE, sourceLoc, type.getKeyword().toUpperCase(),
                expected, actual);
    }
}
