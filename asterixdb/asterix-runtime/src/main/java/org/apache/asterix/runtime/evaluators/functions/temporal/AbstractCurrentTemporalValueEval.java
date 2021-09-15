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

import java.io.DataOutput;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneRules;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.runtime.evaluators.functions.AbstractScalarEval;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

abstract class AbstractCurrentTemporalValueEval extends AbstractScalarEval {

    protected final IEvaluatorContext ctx;
    protected final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected final DataOutput out = resultStorage.getDataOutput();
    protected final GregorianCalendarSystem cal = GregorianCalendarSystem.getInstance();

    private long jobStartTime = Long.MIN_VALUE;
    private ZoneId jobStartTimeZoneId;
    private ZoneRules jobStartTimeZoneRules;

    AbstractCurrentTemporalValueEval(IEvaluatorContext ctx, SourceLocation sourceLoc, FunctionIdentifier funcId) {
        super(sourceLoc, funcId);
        this.ctx = ctx;
    }

    protected final long getSystemCurrentTimeAsAdjustedChronon() throws HyracksDataException {
        return getChrononAdjusted(System.currentTimeMillis());
    }

    protected final long getJobStartTimeAsAdjustedChronon() throws HyracksDataException {
        return getChrononAdjusted(getJobStartTime());
    }

    private long getJobStartTime() throws HyracksDataException {
        if (jobStartTime == Long.MIN_VALUE) {
            IHyracksTaskContext taskCtx = ctx.getTaskContext();
            if (taskCtx == null) {
                throw new HyracksDataException(ErrorCode.ILLEGAL_STATE, srcLoc, "job-start-time");
            }
            jobStartTime = taskCtx.getJobletContext().getJobStartTime();
        }
        return jobStartTime;
    }

    private void ensureJobStartTimeZone() throws HyracksDataException {
        if (jobStartTimeZoneId == null) {
            IHyracksTaskContext taskCtx = ctx.getTaskContext();
            if (taskCtx == null) {
                throw new HyracksDataException(ErrorCode.ILLEGAL_STATE, srcLoc, "job-start-timezone");
            }
            try {
                jobStartTimeZoneId = ZoneId.of(taskCtx.getJobletContext().getJobStartTimeZoneId());
                jobStartTimeZoneRules = jobStartTimeZoneId.getRules();
            } catch (DateTimeException e) {
                throw new HyracksDataException(ErrorCode.ILLEGAL_STATE, e, srcLoc, "job-start-timezone");
            }
        }
    }

    private long getChrononAdjusted(long chronon) throws HyracksDataException {
        ensureJobStartTimeZone();
        ZoneOffset tzOffset = jobStartTimeZoneRules.getOffset(Instant.ofEpochMilli(chronon));
        return cal.adjustChrononByTimezone(chronon, (int) -TimeUnit.SECONDS.toMillis(tzOffset.getTotalSeconds()));
    }
}
