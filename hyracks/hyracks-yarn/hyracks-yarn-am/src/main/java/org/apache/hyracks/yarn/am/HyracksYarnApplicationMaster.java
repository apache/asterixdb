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
package edu.uci.ics.hyracks.yarn.am;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.kohsuke.args4j.CmdLineParser;

import edu.uci.ics.hyracks.yarn.am.manifest.AbstractProcess;
import edu.uci.ics.hyracks.yarn.am.manifest.ContainerSpecification;
import edu.uci.ics.hyracks.yarn.am.manifest.HyracksCluster;
import edu.uci.ics.hyracks.yarn.am.manifest.ManifestParser;
import edu.uci.ics.hyracks.yarn.am.manifest.NodeController;
import edu.uci.ics.hyracks.yarn.common.protocols.amrm.AMRMConnection;

public class HyracksYarnApplicationMaster {
    private final Options options;

    private final Timer timer;

    private final List<ResourceRequest> asks;

    private final Map<Resource, Set<AskRecord>> resource2AskMap;

    private final Map<AbstractProcess, AskRecord> proc2AskMap;

    private final AtomicInteger lastResponseId;

    private final ApplicationAttemptId appAttemptId;

    private YarnConfiguration config;

    private AMRMConnection amrmc;

    private RegisterApplicationMasterResponse registration;

    private HyracksCluster hcManifest;

    private HyracksYarnApplicationMaster(Options options) {
        this.options = options;
        timer = new Timer(true);
        asks = new ArrayList<ResourceRequest>();
        resource2AskMap = new HashMap<Resource, Set<AskRecord>>();
        proc2AskMap = new HashMap<AbstractProcess, AskRecord>();
        lastResponseId = new AtomicInteger();

        String containerIdStr = System.getenv(ApplicationConstants.AM_CONTAINER_ID_ENV);
        ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
        appAttemptId = containerId.getApplicationAttemptId();
    }

    private void run() throws Exception {
        Configuration conf = new Configuration();
        config = new YarnConfiguration(conf);
        amrmc = new AMRMConnection(config);

        performRegistration();
        setupHeartbeats();
        parseManifest();
        setupAsks();
        while (true) {
            Thread.sleep(1000);
        }
    }

    private synchronized void setupAsks() {
        setupAsk(hcManifest.getClusterController());
        for (NodeController nc : hcManifest.getNodeControllers()) {
            setupAsk(nc);
        }
    }

    private void setupAsk(AbstractProcess proc) {
        ContainerSpecification cSpec = proc.getContainerSpecification();
        ResourceRequest rsrcRequest = Records.newRecord(ResourceRequest.class);

        rsrcRequest.setHostName(cSpec.getHostname());

        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(0);
        rsrcRequest.setPriority(pri);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(cSpec.getMemory());
        rsrcRequest.setCapability(capability);

        rsrcRequest.setNumContainers(1);

        AskRecord ar = new AskRecord();
        ar.req = rsrcRequest;
        ar.proc = proc;

        Set<AskRecord> arSet = resource2AskMap.get(capability);
        if (arSet == null) {
            arSet = new HashSet<AskRecord>();
            resource2AskMap.put(capability, arSet);
        }
        arSet.add(ar);
        proc2AskMap.put(proc, ar);

        System.err.println(proc + " -> [" + rsrcRequest.getHostName() + ", " + rsrcRequest.getNumContainers() + ", "
                + rsrcRequest.getPriority() + ", " + rsrcRequest.getCapability().getMemory() + "]");

        asks.add(rsrcRequest);
    }

    private void parseManifest() throws Exception {
        String str = FileUtils.readFileToString(new File("manifest.xml"));
        hcManifest = ManifestParser.parse(str);
    }

    private void setupHeartbeats() {
        long heartbeatInterval = config.getLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
                YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS);
        System.err.println("Heartbeat interval: " + heartbeatInterval);
        heartbeatInterval = Math.min(heartbeatInterval, 1000);
        System.err.println("Heartbeat interval: " + heartbeatInterval);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                AllocateRequest hb = Records.newRecord(AllocateRequest.class);
                populateAllocateRequest(hb);
                hb.setApplicationAttemptId(amrmc.getApplicationAttemptId());
                hb.setProgress(0);
                try {
                    AllocateResponse allocateResponse = amrmc.getAMRMProtocol().allocate(hb);
                    List<Container> allocatedContainers = allocateResponse.getAMResponse().getAllocatedContainers();
                    List<ContainerStatus> completedContainers = allocateResponse.getAMResponse()
                            .getCompletedContainersStatuses();
                    processAllocation(allocatedContainers, completedContainers);
                } catch (YarnRemoteException e) {
                    e.printStackTrace();
                }
            }
        }, 0, heartbeatInterval);
    }

    private synchronized void populateAllocateRequest(AllocateRequest hb) {
        hb.addAllAsks(asks);
        hb.addAllReleases(new ArrayList<ContainerId>());
        hb.setResponseId(lastResponseId.incrementAndGet());
        hb.setApplicationAttemptId(appAttemptId);
    }

    private synchronized void processAllocation(List<Container> allocatedContainers,
            List<ContainerStatus> completedContainers) {
        System.err.println(allocatedContainers);
        for (Container c : allocatedContainers) {
            System.err.println("Got container: " + c.getContainerStatus());
            NodeId nodeId = c.getNodeId();
            Resource resource = c.getResource();

            Set<AskRecord> arSet = resource2AskMap.get(resource);
            boolean found = false;
            if (arSet != null) {
                AskRecord wildcardMatch = null;
                AskRecord nameMatch = null;
                for (AskRecord ar : arSet) {
                    ResourceRequest req = ar.req;
                    if (ar.allocation == null) {
                        if ("*".equals(req.getHostName()) && wildcardMatch == null) {
                            wildcardMatch = ar;
                        }
                        if (req.getHostName().equals(nodeId.getHost()) && nameMatch == null) {
                            nameMatch = ar;
                            break;
                        }
                    }
                }
                if (nameMatch != null) {
                    found = true;
                    nameMatch.allocation = c;
                } else if (wildcardMatch != null) {
                    found = true;
                    wildcardMatch.allocation = c;
                }
            }
            if (!found) {
                System.err.println("Unknown request satisfied: " + resource);
            }
        }
    }

    private void performRegistration() throws YarnRemoteException {
        RegisterApplicationMasterRequest appMasterRequest = Records.newRecord(RegisterApplicationMasterRequest.class);
        appMasterRequest.setApplicationAttemptId(amrmc.getApplicationAttemptId());

        registration = amrmc.getAMRMProtocol().registerApplicationMaster(appMasterRequest);
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        try {
            parser.parseArgument(args);
        } catch (Exception e) {
            parser.printUsage(System.err);
            return;
        }
        new HyracksYarnApplicationMaster(options).run();
    }

    private static class Options {
    }

    private static class AskRecord {
        ResourceRequest req;
        AbstractProcess proc;
        Container allocation;
    }
}