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
package org.apache.hyracks.test.http;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.internal.PlatformDependent;

public class HttpTestUtil {
    private static final Logger LOGGER = LogManager.getLogger();
    private static long maxMemUsage = 0L;

    public static synchronized void printMemUsage() {
        StringBuilder report = new StringBuilder();
        /* TODO: unsupported APIs after java 8
        report.append("sun.misc.VM.maxDirectMemory: ");
        report.append(sun.misc.VM.maxDirectMemory());
        report.append('\n');
        report.append("sun.misc.SharedSecrets.getJavaNioAccess().getDirectBufferPool().getMemoryUsed(): ");
        report.append(sun.misc.SharedSecrets.getJavaNioAccess().getDirectBufferPool().getMemoryUsed());
        report.append('\n');
        report.append("sun.misc.SharedSecrets.getJavaNioAccess().getDirectBufferPool().getTotalCapacity(): ");
        report.append(sun.misc.SharedSecrets.getJavaNioAccess().getDirectBufferPool().getTotalCapacity());
        */
        report.append('\n');
        report.append("ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage(): ");
        report.append(ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage());
        report.append('\n');
        report.append("---------------------------- Beans ----------------------------");
        report.append('\n');
        List<MemoryPoolMXBean> memPoolBeans = ManagementFactory.getMemoryPoolMXBeans();
        for (MemoryPoolMXBean bean : memPoolBeans) {
            if (bean.isValid() && bean.getType() == MemoryType.NON_HEAP) {
                report.append(bean.getName());
                report.append(": ");
                report.append(bean.getUsage());
                report.append('\n');
            }
        }
        report.append("---------------------------- Netty ----------------------------");
        report.append('\n');
        try {
            Field field = PlatformDependent.class.getDeclaredField("DIRECT_MEMORY_COUNTER");
            field.setAccessible(true);
            AtomicLong usedDirectMemory = (AtomicLong) field.get(null);
            long used = usedDirectMemory.get();
            report.append("Current PlatformDependent.DIRECT_MEMORY_COUNTER: ");
            report.append(used);
            report.append('\n');
            report.append("Maximum PlatformDependent.DIRECT_MEMORY_COUNTER: ");
            maxMemUsage = Math.max(maxMemUsage, used);
            report.append(maxMemUsage);
            report.append('\n');
            report.append('\n');
        } catch (Throwable th) { // NOSONAR
            LOGGER.log(Level.WARN, "Failed to access PlatformDependent.DIRECT_MEMORY_COUNTER", th);
            return;
        }
        report.append("--------------- PooledByteBufAllocator.DEFAULT ----------------");
        report.append(PooledByteBufAllocator.DEFAULT.dumpStats());
        LOGGER.log(Level.INFO, report.toString());
    }
}
