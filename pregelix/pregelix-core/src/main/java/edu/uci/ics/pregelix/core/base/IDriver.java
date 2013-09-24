/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.pregelix.core.base;

import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public interface IDriver {

    public static enum Plan {
        INNER_JOIN,
        OUTER_JOIN,
        OUTER_JOIN_SORT,
        OUTER_JOIN_SINGLE_SORT
    }

    public void runJob(PregelixJob job, String ipAddress, int port) throws HyracksException;

    public void runJobs(List<PregelixJob> jobs, String ipAddress, int port) throws HyracksException;

    public void runJob(PregelixJob job, Plan planChoice, String ipAddress, int port, boolean profiling)
            throws HyracksException;

    public void runJobs(List<PregelixJob> jobs, Plan planChoice, String ipAddress, int port, boolean profiling)
            throws HyracksException;
}
