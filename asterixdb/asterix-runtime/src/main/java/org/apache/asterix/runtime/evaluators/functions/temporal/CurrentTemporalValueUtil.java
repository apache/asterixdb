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

import java.time.Instant;
import java.time.zone.ZoneRules;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.types.ATypeTag;

public final class CurrentTemporalValueUtil {

    private static final GregorianCalendarSystem CAL = GregorianCalendarSystem.getInstance();

    private CurrentTemporalValueUtil() {
    }

    /**
     * The value a current date, time or datetime function produces for an instant read in a zone.
     *
     * @param type {@link ATypeTag#DATE}, {@link ATypeTag#TIME} or {@link ATypeTag#DATETIME}
     * @param epochMillis the instant
     * @param zoneRules the zone the instant is read in
     * @return the date chronon (days since the epoch), the time chronon (milliseconds into the day) or the
     *         datetime chronon
     */
    public static long valueAt(ATypeTag type, long epochMillis, ZoneRules zoneRules) {
        int offsetMillis = (int) TimeUnit.SECONDS
                .toMillis(zoneRules.getOffset(Instant.ofEpochMilli(epochMillis)).getTotalSeconds());
        long local = CAL.adjustChrononByTimezone(epochMillis, -offsetMillis);
        switch (type) {
            case DATE:
                return CAL.getChrononInDays(local);
            case TIME:
                return CAL.getTimeChronon(local);
            case DATETIME:
                return local;
            default:
                throw new IllegalArgumentException(type.toString());
        }
    }
}
