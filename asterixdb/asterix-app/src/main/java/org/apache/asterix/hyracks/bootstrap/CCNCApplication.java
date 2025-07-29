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
package org.apache.asterix.hyracks.bootstrap;

import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.hyracks.control.common.config.ConfigUtils;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;

public class CCNCApplication {

    ClusterControllerService ccSvc;
    NodeControllerService ncSvc;

    public static void main(String[] args) throws Exception {
        CCNCApplication apps = new CCNCApplication();
        apps.createServices(args);
        apps.startServices();
        while (true) {
            Thread.sleep(10000);
        }
    }

    public void createServices(String[] args) throws Exception {
        int ccArgIdx = ArrayUtils.indexOf(args, "--cc");
        int ncArgIdx = ArrayUtils.indexOf(args, "--nc");
        int[] allIdx = ArrayUtils.removeAllOccurrences(new int[] { ccArgIdx, ncArgIdx }, -1);
        Arrays.sort(allIdx);
        final ConfigManager cfgMgr = new ConfigManager(getArgs(args, ncArgIdx, allIdx));
        String nodeId = ConfigUtils.getOptionValue(getArgs(args, ncArgIdx, allIdx), NCConfig.Option.NODE_ID);
        NCApplication ncApp = new NCApplication();
        ncApp.registerConfig(cfgMgr);
        NCConfig ncCfg = new NCConfig(nodeId, cfgMgr);
        cfgMgr.processConfig();
        ncSvc = new NodeControllerService(ncCfg, ncApp);
        createCC(getArgs(args, ccArgIdx, allIdx), ncSvc);
    }

    public void createCC(String[] args, NodeControllerService ncSvc) throws Exception {
        String nodeId = ncSvc.getId();
        CCApplication ccApp = new CCApplication();
        final ConfigManager cfgMgr = new ConfigManager(args);
        ccApp.registerConfig(cfgMgr);
        CCConfig ccCfg = new CCConfig(cfgMgr);
        cfgMgr.processConfig();
        cfgMgr.ensureNode(nodeId);
        ccSvc = new ClusterControllerService(ccCfg, ccApp);
    }

    private String[] getArgs(String[] args, int idx, int[] allIdx) {
        String[] subArgs = ArrayUtils.subarray(args, idx + 1, getEnd(idx, allIdx));
        if (allIdx[0] != 0) {
            subArgs = ArrayUtils.addAll(subArgs, ArrayUtils.subarray(args, 0, allIdx[0]));
        }
        return subArgs;
    }

    private int getEnd(int idx, int[] idxes) {
        int cut = Arrays.binarySearch(idxes, idx + 1);
        if (cut >= 0) {
            return cut + 1;
        }
        cut = -cut - 1;
        return cut >= idxes.length ? Integer.MAX_VALUE : idxes[cut];
    }

    public void startServices() throws Exception {
        ccSvc.start();
        ncSvc.start();
    }
}
