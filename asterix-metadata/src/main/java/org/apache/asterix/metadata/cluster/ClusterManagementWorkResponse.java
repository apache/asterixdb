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
package edu.uci.ics.asterix.metadata.cluster;

import edu.uci.ics.asterix.common.api.IClusterManagementWork;
import edu.uci.ics.asterix.common.api.IClusterManagementWorkResponse;

public class ClusterManagementWorkResponse implements IClusterManagementWorkResponse {

    protected final IClusterManagementWork work;

    protected Status status;

    public ClusterManagementWorkResponse(IClusterManagementWork w) {
        this.work = w;
        this.status = Status.IN_PROGRESS;
    }

   
    @Override
    public IClusterManagementWork getWork() {
        return work;
    }

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public void setStatus(Status status) {
        this.status = status;
    }

}
