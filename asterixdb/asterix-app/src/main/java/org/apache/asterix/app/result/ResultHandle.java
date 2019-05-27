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
package org.apache.asterix.app.result;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.ResultSetId;

public class ResultHandle {
    private final JobId jobId;
    private final ResultSetId resultSetId;

    public ResultHandle(JobId jobId, ResultSetId resultSetId) {
        this.jobId = jobId;
        this.resultSetId = resultSetId;
    }

    public ResultHandle(long jobId, long resultSetId) {
        this(new JobId(jobId), new ResultSetId(resultSetId));
    }

    public static ResultHandle parse(String str) {
        int dash = str.indexOf('-');
        if (dash < 1) {
            return null;
        }
        int start = 0;
        while (str.charAt(start) == '/') {
            ++start;
        }
        String jobIdStr = str.substring(start, dash);
        String resIdStr = str.substring(dash + 1);
        try {
            return new ResultHandle(Long.parseLong(jobIdStr), Long.parseLong(resIdStr));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public JobId getJobId() {
        return jobId;
    }

    public ResultSetId getResultSetId() {
        return resultSetId;
    }

    @Override
    public String toString() {
        return jobId.getId() + "-" + resultSetId.getId();
    }
}
