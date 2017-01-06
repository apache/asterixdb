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
package org.apache.hyracks.control.common.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ThreadDumpHelper {

    private ThreadDumpHelper() {
    }

    public static String takeDumpJSON(ThreadMXBean threadMXBean) throws IOException {
        ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
        List<Map<String, Object>> threads = new ArrayList<>();

        for (ThreadInfo thread : threadInfos) {
            Map<String, Object> threadMap = new HashMap<>();
            threadMap.put("name", thread.getThreadName());
            threadMap.put("id", thread.getThreadId());
            threadMap.put("state", thread.getThreadState().name());
            List<String> stacktrace = new ArrayList<>();
            for (StackTraceElement element : thread.getStackTrace()) {
                stacktrace.add(element.toString());
            }
            threadMap.put("stack", stacktrace);

            if (thread.getLockName() != null) {
                threadMap.put("lock_name", thread.getLockName());
            }
            if (thread.getLockOwnerId() != -1) {
                threadMap.put("lock_owner_id", thread.getLockOwnerId());
            }
            if (thread.getBlockedTime() > 0) {
                threadMap.put("blocked_time", thread.getBlockedTime());
            }
            if (thread.getBlockedCount() > 0) {
                threadMap.put("blocked_count", thread.getBlockedCount());
            }
            if (thread.getLockedMonitors().length > 0) {
                threadMap.put("locked_monitors", thread.getLockedMonitors());
            }
            if (thread.getLockedSynchronizers().length > 0) {
                threadMap.put("locked_synchronizers", thread.getLockedSynchronizers());
            }
            threads.add(threadMap);
        }
        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();
        json.put("date", new Date().toString());
        json.putPOJO("threads", threads);

        long[] deadlockedThreads = threadMXBean.findDeadlockedThreads();
        long[] monitorDeadlockedThreads = threadMXBean.findMonitorDeadlockedThreads();
        if (deadlockedThreads != null && deadlockedThreads.length > 0) {
            json.putPOJO("deadlocked_thread_ids", deadlockedThreads);
        }
        if (monitorDeadlockedThreads != null && monitorDeadlockedThreads.length > 0) {
            json.putPOJO("monitor_deadlocked_thread_ids", monitorDeadlockedThreads);
        }
        om.enable(SerializationFeature.INDENT_OUTPUT);
        return om.writerWithDefaultPrettyPrinter().writeValueAsString(json);
    }
}
