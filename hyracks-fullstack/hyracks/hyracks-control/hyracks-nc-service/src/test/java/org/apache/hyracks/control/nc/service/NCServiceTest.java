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
package org.apache.hyracks.control.nc.service;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import junit.extensions.PA;

public class NCServiceTest {

    @Test
    public void testJvmArgs() {
        Map<String, String> configuration = new HashMap<>();
        OperatingSystemMXBean osMXBean = ManagementFactory.getOperatingSystemMXBean();
        PA.setValue(NCService.class, "nodeSection", "nc/nc1");
        PA.setValue(NCService.class, "osMXBean", osMXBean);
        PA.invokeMethod(NCService.class, "configEnvironment(java.util.Map)", configuration);
        String javaOpts = configuration.get("JAVA_OPTS");
        String prefix = javaOpts.substring(4);
        String sizeStr = prefix.substring(0, prefix.length() - 1);
        int size = Integer.parseInt(sizeStr);
        long ramSize = ((com.sun.management.OperatingSystemMXBean) osMXBean).getTotalPhysicalMemorySize();
        int base = 1024 * 1024 * 5;
        Assert.assertTrue(size == ramSize * 3 / base + ((ramSize * 3) % base == 0 ? 0 : 1));
    }
}
