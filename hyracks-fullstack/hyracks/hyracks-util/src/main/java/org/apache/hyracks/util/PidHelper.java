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

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PidHelper {

    private static final Logger LOGGER = LogManager.getLogger();

    private PidHelper() {
    }

    public static long getPid() {
        return getPid(ManagementFactory.getRuntimeMXBean());
    }

    public static long getPid(RuntimeMXBean runtimeMXBean) {
        // TODO: replace with direct invoke of getPid() once compatibility is at JDK 10 or higher
        try {
            Method getPidMethod = runtimeMXBean.getClass().getMethod("getPid");
            return (Long) getPidMethod.invoke(runtimeMXBean);
        } catch (NoSuchMethodException e) {
            LOGGER.debug("ignoring exception trying to find getPid() (expected pre-JDK 10)", e);
        } catch (IllegalAccessException | InvocationTargetException e) {
            LOGGER.debug("ignoring exception trying to execute getPid()", e);
        }
        try {
            Field jvmField = runtimeMXBean.getClass().getDeclaredField("jvm");
            jvmField.setAccessible(true);
            Object vmManagement = jvmField.get(runtimeMXBean);
            Method getProcessIdMethod = vmManagement.getClass().getDeclaredMethod("getProcessId");
            getProcessIdMethod.setAccessible(true);
            return (Integer) getProcessIdMethod.invoke(vmManagement);
        } catch (Exception e) {
            LOGGER.log(Level.INFO, "Unable to determine PID due to exception", e);
            return -1;
        }
    }
}
