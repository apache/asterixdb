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
package org.apache.hyracks.control.nc.work;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.comm.PartitionChannel;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.comm.channels.NetworkInputChannel;
import org.apache.hyracks.control.common.work.AbstractWork;
import org.apache.hyracks.control.nc.Joblet;
import org.apache.hyracks.control.nc.NodeControllerService;

public class ReportPartitionAvailabilityWork extends AbstractWork {
    private final NodeControllerService ncs;

    private final PartitionId pid;

    private final NetworkAddress networkAddress;

    public ReportPartitionAvailabilityWork(NodeControllerService ncs, PartitionId pid, NetworkAddress networkAddress) {
        this.ncs = ncs;
        this.pid = pid;
        this.networkAddress = networkAddress;
    }

    @Override
    public void run() {
        try {
            Map<JobId, Joblet> jobletMap = ncs.getJobletMap();
            Joblet ji = jobletMap.get(pid.getJobId());
            if (ji != null) {
                PartitionChannel channel =
                        new PartitionChannel(pid,
                                new NetworkInputChannel(ncs.getNetworkManager(),
                                        new InetSocketAddress(
                                                InetAddress.getByAddress(networkAddress.lookupIpAddress()),
                                                networkAddress.getPort()),
                                        pid, 5));
                ji.reportPartitionAvailability(channel);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
