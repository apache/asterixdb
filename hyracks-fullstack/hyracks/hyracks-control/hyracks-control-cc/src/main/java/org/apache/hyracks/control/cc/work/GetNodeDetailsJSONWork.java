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

package org.apache.hyracks.control.cc.work;

import static org.apache.hyracks.control.common.utils.ConfigurationUtil.toPathElements;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.common.config.ConfigUtils;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.work.IPCResponder;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.hyracks.util.MXHelper;
import org.apache.hyracks.util.PidHelper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class GetNodeDetailsJSONWork extends SynchronizableWork {

    private static final Section[] CC_SECTIONS = { Section.CC, Section.COMMON };
    private static final Section[] NC_SECTIONS = { Section.NC, Section.COMMON };

    private final INodeManager nodeManager;
    private final CCConfig ccConfig;
    private final String nodeId;
    private final boolean includeStats;
    private final boolean includeConfig;
    private final IPCResponder<String> callback;
    private ObjectNode detail;
    private ObjectMapper om = new ObjectMapper();

    public GetNodeDetailsJSONWork(INodeManager nodeManager, CCConfig ccConfig, String nodeId, boolean includeStats,
            boolean includeConfig, IPCResponder<String> callback) {
        this.nodeManager = nodeManager;
        this.ccConfig = ccConfig;
        this.nodeId = nodeId;
        this.includeStats = includeStats;
        this.includeConfig = includeConfig;
        this.callback = callback;
    }

    public GetNodeDetailsJSONWork(INodeManager nodeManager, CCConfig ccConfig, String nodeId, boolean includeStats,
            boolean includeConfig) {
        this(nodeManager, ccConfig, nodeId, includeStats, includeConfig, null);
    }

    @Override
    protected void doRun() throws Exception {
        if (nodeId == null) {
            // null nodeId is a request for CC
            detail = getCCDetails();
            if (includeConfig) {
                ConfigUtils.addConfigToJSON(detail, ccConfig.getAppConfig(), CC_SECTIONS);
                detail.putPOJO("app.args", ccConfig.getAppArgs());
            }
        } else {
            NodeControllerState ncs = nodeManager.getNodeControllerState(nodeId);
            if (ncs != null) {
                detail = ncs.toDetailedJSON(includeStats, includeConfig);
                if (includeConfig) {
                    ConfigUtils.addConfigToJSON(detail, ncs.getConfig(), ccConfig.getConfigManager(), NC_SECTIONS);
                }
            }
        }

        if (callback != null) {
            callback.setValue(detail == null ? null : om.writeValueAsString(detail));
        }
    }

    private ObjectNode getCCDetails() {
        ObjectNode o = om.createObjectNode();
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        List<GarbageCollectorMXBean> gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        OperatingSystemMXBean osMXBean = ManagementFactory.getOperatingSystemMXBean();
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();

        if (includeConfig) {
            o.put("os_name", osMXBean.getName());
            o.put("arch", osMXBean.getArch());
            o.put("os_version", osMXBean.getVersion());
            o.put("num_processors", osMXBean.getAvailableProcessors());
            o.put("vm_name", runtimeMXBean.getVmName());
            o.put("vm_version", runtimeMXBean.getVmVersion());
            o.put("vm_vendor", runtimeMXBean.getVmVendor());
            o.putPOJO("classpath", toPathElements(runtimeMXBean.getClassPath()));
            o.putPOJO("library_path", toPathElements(runtimeMXBean.getLibraryPath()));
            o.putPOJO("boot_classpath", toPathElements(MXHelper.getBootClassPath()));
            o.putPOJO("input_arguments", runtimeMXBean.getInputArguments());
            o.putPOJO("system_properties", runtimeMXBean.getSystemProperties());
            o.put("pid", PidHelper.getPid());
        }
        if (includeStats) {
            MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
            MemoryUsage nonheapUsage = memoryMXBean.getNonHeapMemoryUsage();

            List<ObjectNode> gcs = new ArrayList<>();

            for (GarbageCollectorMXBean gcMXBean : gcMXBeans) {
                ObjectNode gc = om.createObjectNode();
                gc.put("name", gcMXBean.getName());
                gc.put("collection-time", gcMXBean.getCollectionTime());
                gc.put("collection-count", gcMXBean.getCollectionCount());
                gcs.add(gc);
            }
            o.putPOJO("gcs", gcs);

            o.put("date", new Date().toString());
            o.put("heap_init_size", heapUsage.getInit());
            o.put("heap_used_size", heapUsage.getUsed());
            o.put("heap_committed_size", heapUsage.getCommitted());
            o.put("heap_max_size", heapUsage.getMax());
            o.put("nonheap_init_size", nonheapUsage.getInit());
            o.put("nonheap_used_size", nonheapUsage.getUsed());
            o.put("nonheap_committed_size", nonheapUsage.getCommitted());
            o.put("nonheap_max_size", nonheapUsage.getMax());
            o.put("thread_count", threadMXBean.getThreadCount());
            o.put("peak_thread_count", threadMXBean.getPeakThreadCount());
            o.put("started_thread_count", threadMXBean.getTotalStartedThreadCount());
            o.put("system_load_average", osMXBean.getSystemLoadAverage());
        }
        return o;
    }

    public ObjectNode getDetail() {
        return detail;
    }
}
