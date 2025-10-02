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
    private final String requestId;
    private final int partition;

    public ResultHandle(JobId jobId, ResultSetId resultSetId, String requestId, int partition) {
        this.jobId = jobId;
        this.resultSetId = resultSetId;
        this.requestId = requestId;
        this.partition = partition;
    }

    public ResultHandle(JobId jobId, ResultSetId resultSetId, String requestId) {
        this(jobId, resultSetId, requestId, -1);
    }

    public ResultHandle(long jobId, long resultSetId, String requestId, int partition) {
        this(new JobId(jobId), new ResultSetId(resultSetId), requestId, partition);
    }

    public static ResultHandle parse(String str) {
        int start = 0;
        while (start < str.length() && str.charAt(start) == '/') {
            ++start;
        }
        str = str.substring(start);
        String[] pathParts = str.split("/");

        switch (pathParts.length) {
            case 1:
                // Format: jobId-resultSetId
                return parseJobAndResultId(pathParts[0], null, -1);
            case 2:
                // Format: requestId/jobId-resultSetId
                return parseJobAndResultId(pathParts[1], pathParts[0], -1);
            case 3:
                // Format: requestId/jobId-resultSetId/partition
                try {
                    int partition = Integer.parseInt(pathParts[2]);
                    if (partition < 0) {
                        return null;
                    }
                    return parseJobAndResultId(pathParts[1], pathParts[0], partition);
                } catch (NumberFormatException e) {
                    return null;
                }
            default:
                return null;
        }
    }

    private static ResultHandle parseJobAndResultId(String jobResPart, String requestId, int partition) {
        int dash = jobResPart.indexOf('-');
        if (dash < 1) {
            return null;
        }

        String jobIdStr = jobResPart.substring(0, dash);
        String resIdStr = jobResPart.substring(dash + 1);

        try {
            long jobId = Long.parseLong(jobIdStr);
            long resultSetId = Long.parseLong(resIdStr);
            return new ResultHandle(jobId, resultSetId, requestId, partition);
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

    public String getRequestId() {
        return requestId;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public String toString() {
        if (requestId == null) {
            return jobId.getId() + "-" + resultSetId.getId();
        } else {
            return requestId + "/" + jobId.getId() + "-" + resultSetId.getId();
        }
    }
}
