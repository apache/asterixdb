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
package org.apache.hyracks.api.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.control.CcIdPartitionedLongFactory;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IWritable;

public final class JobId implements IWritable, Serializable, Comparable {

    private static final Pattern jobIdPattern = Pattern.compile("^JID:(\\d+)\\.(\\d+)$");

    public static final JobId INVALID = null;

    private static final long serialVersionUID = 1L;
    private long id;
    private transient volatile CcId ccId;

    public static JobId create(DataInput dis) throws IOException {
        JobId jobId = new JobId();
        jobId.readFields(dis);
        return jobId;
    }

    private JobId() {
    }

    public JobId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public CcId getCcId() {
        if (ccId == null) {
            ccId = CcId.valueOf((int) (id >>> CcIdPartitionedLongFactory.ID_BITS));
        }
        return ccId;
    }

    public long getIdOnly() {
        return id & CcIdPartitionedLongFactory.MAX_ID;
    }

    @Override
    public int hashCode() {
        return (int) id;
    }

    @Override
    public boolean equals(Object o) {
        return o == this || o instanceof JobId && ((JobId) o).id == id;
    }

    @Override
    public String toString() {
        return "JID:" + (id >>> CcIdPartitionedLongFactory.ID_BITS) + "." + getIdOnly();
    }

    public static JobId parse(String str) throws HyracksDataException {
        Matcher m = jobIdPattern.matcher(str);
        if (m.matches()) {
            int ccId = Integer.parseInt(m.group(1));
            if (ccId <= 0xffff && ccId >= 0) {
                long jobId = Long.parseLong(m.group(2)) | (long) ccId << CcIdPartitionedLongFactory.ID_BITS;
                return new JobId(jobId);
            }
        }
        throw HyracksDataException.create(ErrorCode.NOT_A_JOBID, str);
    }

    @Override
    public void writeFields(DataOutput output) throws IOException {
        output.writeLong(id);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        id = input.readLong();
    }

    @Override
    public int compareTo(Object other) {
        return Long.compare(id, ((JobId) other).id);
    }
}
