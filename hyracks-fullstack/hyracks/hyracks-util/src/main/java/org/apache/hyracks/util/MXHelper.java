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
package org.apache.hyracks.util;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MXHelper {
    private static final Logger LOGGER = LogManager.getLogger();
    public static final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    public static final List<GarbageCollectorMXBean> gcMXBeans =
            Collections.unmodifiableList(ManagementFactory.getGarbageCollectorMXBeans());
    public static final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    public static final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    public static final OperatingSystemMXBean osMXBean = ManagementFactory.getOperatingSystemMXBean();
    private static Method getOpenFileDescriptorCount;
    private static Method getMaxFileDescriptorCount;

    static {
        if (SystemUtils.IS_OS_WINDOWS) {
            LOGGER.info("Access to file descriptors (FDs) is not available on Windows; FD info will not be logged");
        } else {
            Class<? extends OperatingSystemMXBean> osMXBeanClass = osMXBean.getClass();
            try {
                getOpenFileDescriptorCount = osMXBeanClass.getMethod("getOpenFileDescriptorCount");
                getMaxFileDescriptorCount = osMXBeanClass.getDeclaredMethod("getMaxFileDescriptorCount");
                getOpenFileDescriptorCount.setAccessible(true);
                getMaxFileDescriptorCount.setAccessible(true);
            } catch (Throwable th) { // NOSONAR: diagnostic code shouldn't cause server failure
                getOpenFileDescriptorCount = null;
                getMaxFileDescriptorCount = null;
                LOGGER.warn("Failed setting up the methods to get the number of file descriptors through {}",
                        osMXBeanClass.getName(), th);
            }
        }
    }

    private MXHelper() {
    }

    public static boolean supportOpenFileCount() {
        return getOpenFileDescriptorCount != null;
    }

    public static long getCurrentOpenFileCount() {
        if (getOpenFileDescriptorCount == null) {
            return -1;
        }
        try {
            return (long) getOpenFileDescriptorCount.invoke(osMXBean);
        } catch (Throwable e) { // NOSONAR
            LOGGER.log(Level.WARN, "Failure invoking getOpenFileDescriptorCount", e);
            return -1;
        }
    }

    public static long getMaxOpenFileCount() {
        if (getMaxFileDescriptorCount == null) {
            return -1;
        }
        try {
            return (long) getMaxFileDescriptorCount.invoke(osMXBean);
        } catch (Throwable e) { // NOSONAR
            LOGGER.log(Level.WARN, "Failure invoking getMaxFileDescriptorCount", e);
            return -1;
        }
    }

    public static void logFileDescriptors() {
        try {
            if (supportOpenFileCount()) {
                LOGGER.log(Level.WARN,
                        "Number of open files by this process is {}. Max number of allowed open files is {} ",
                        MXHelper::getCurrentOpenFileCount, MXHelper::getMaxOpenFileCount);
            }
        } catch (Throwable th) { // NOSONAR: diagnostic code shouldn't cause server failure
            LOGGER.log(Level.WARN, "Failed getting the count of open files", th);
        }
    }

    public static String getBootClassPath() {
        try {
            return runtimeMXBean.getBootClassPath();
        } catch (UnsupportedOperationException e) {
            // boot classpath is not supported in Java 9 and later
            LOGGER.debug("ignoring exception calling RuntimeMXBean.getBootClassPath; returning null", e);
            return null;
        }
    }
}
