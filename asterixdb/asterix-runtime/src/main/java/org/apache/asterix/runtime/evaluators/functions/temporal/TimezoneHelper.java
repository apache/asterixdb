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

package org.apache.asterix.runtime.evaluators.functions.temporal;

import java.time.zone.ZoneRules;
import java.util.TimeZone;

import org.apache.asterix.om.base.temporal.DateTimeFormatUtils;
import org.apache.asterix.runtime.evaluators.functions.StringEvaluatorUtils;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;

final class TimezoneHelper {

    private final SourceLocation srcLoc;

    private final FunctionIdentifier funID;

    private final ByteArrayAccessibleOutputStream lastTimezoneIdStorage = new ByteArrayAccessibleOutputStream();

    private final UTF8StringPointable lastTimezoneIdPtr = new UTF8StringPointable();

    private ZoneRules lastTimezoneRules;

    TimezoneHelper(SourceLocation srcLoc, FunctionIdentifier funID) {
        this.srcLoc = srcLoc;
        this.funID = funID;
    }

    public ZoneRules parseTimeZone(UTF8StringPointable timezoneIdPtr) throws InvalidDataFormatException {
        boolean newTimeZoneId = lastTimezoneRules == null || lastTimezoneIdPtr.compareTo(timezoneIdPtr) != 0;
        if (newTimeZoneId) {
            TimeZone tz = DateTimeFormatUtils.findTimeZone(timezoneIdPtr.getByteArray(),
                    timezoneIdPtr.getCharStartOffset(), timezoneIdPtr.getUTF8Length());
            if (tz == null) {
                throw new InvalidDataFormatException(srcLoc, funID, "timezone");
            }
            // ! object creation !
            lastTimezoneRules = tz.toZoneId().getRules();
            StringEvaluatorUtils.copyResetUTF8Pointable(timezoneIdPtr, lastTimezoneIdStorage, lastTimezoneIdPtr);
        }
        return lastTimezoneRules;
    }
}
