/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.control.nc.work;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.comm.PartitionChannel;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.common.work.SynchronizableWork;
import edu.uci.ics.hyracks.control.nc.Joblet;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.control.nc.net.NetworkInputChannel;

public class ReportPartitionAvailabilityWork extends SynchronizableWork {
    private static final Logger LOGGER = Logger.getLogger(ReportPartitionAvailabilityWork.class.getName());

    private final NodeControllerService ncs;

    private final PartitionId pid;

    private final NetworkAddress networkAddress;

    public ReportPartitionAvailabilityWork(NodeControllerService ncs, PartitionId pid, NetworkAddress networkAddress) {
        this.ncs = ncs;
        this.pid = pid;
        this.networkAddress = networkAddress;
    }

    @Override
    protected void doRun() throws Exception {
        Map<JobId, Joblet> jobletMap = ncs.getJobletMap();
        Joblet ji = jobletMap.get(pid.getJobId());
        if (ji != null) {
            PartitionChannel channel = new PartitionChannel(pid, new NetworkInputChannel(ncs.getRootContext(),
                    ncs.getNetworkManager(), new InetSocketAddress(networkAddress.getIpAddress(),
                            networkAddress.getPort()), pid, 1));
            ji.reportPartitionAvailability(channel);
        }
    }
}