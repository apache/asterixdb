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
package org.apache.hyracks.http.servlet;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.internal.PlatformDependent;

public class ChattyServlet extends AbstractServlet {
    private static final Logger LOGGER = Logger.getLogger(ChattyServlet.class.getName());
    private static long MAX = 0L;
    private byte[] bytes;

    public ChattyServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
        String line = "I don't know when to stop talking\n";
        StringBuilder responseBuilder = new StringBuilder();
        for (int i = 0; i < 100000; i++) {
            responseBuilder.append(line);
        }
        String responseString = responseBuilder.toString();
        bytes = responseString.getBytes();
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) throws Exception {
        get(request, response);
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws Exception {
        response.setStatus(HttpResponseStatus.OK);
        HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_HTML, HttpUtil.Encoding.UTF8);
        LOGGER.log(Level.WARNING, "I am about to flood you... and a single buffer is " + bytes.length + " bytes");
        for (int i = 0; i < 100; i++) {
            response.outputStream().write(bytes);
        }
        printMemUsage();
    }

    @SuppressWarnings("restriction")
    public synchronized static void printMemUsage() {
        StringBuilder report = new StringBuilder();
        report.append("sun.misc.VM.maxDirectMemory: ");
        report.append(sun.misc.VM.maxDirectMemory());
        report.append('\n');
        report.append("sun.misc.SharedSecrets.getJavaNioAccess().getDirectBufferPool().getMemoryUsed(): ");
        report.append(sun.misc.SharedSecrets.getJavaNioAccess().getDirectBufferPool().getMemoryUsed());
        report.append('\n');
        report.append("sun.misc.SharedSecrets.getJavaNioAccess().getDirectBufferPool().getTotalCapacity(): ");
        report.append(sun.misc.SharedSecrets.getJavaNioAccess().getDirectBufferPool().getTotalCapacity());
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
            MAX = Math.max(MAX, used);
            report.append(MAX);
            report.append('\n');
            report.append('\n');
        } catch (Throwable th) {
            th.printStackTrace();
            LOGGER.log(Level.WARNING, "Failed to access PlatformDependent.DIRECT_MEMORY_COUNTER", th);
            return;
        }
        report.append("--------------- PooledByteBufAllocator.DEFAULT ----------------");
        report.append(PooledByteBufAllocator.DEFAULT.dumpStats());
        LOGGER.log(Level.INFO, report.toString());
    }
}