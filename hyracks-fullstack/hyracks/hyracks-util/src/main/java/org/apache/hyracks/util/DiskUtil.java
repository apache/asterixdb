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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DiskUtil {

    private static final Logger LOGGER = LogManager.getLogger();

    private DiskUtil() {
        throw new AssertionError("Util class should not be initialized.");
    }

    /**
     * Mounts a RAM disk
     *
     * @param name
     * @param size
     * @param unit
     * @return The root of the mounted disk
     * @throws IOException
     * @throws InterruptedException
     */
    public static Path mountRamDisk(String name, int size, StorageUtil.StorageUnit unit)
            throws IOException, InterruptedException {
        if (SystemUtils.IS_OS_MAC) {
            return mountMacRamDisk(name, (StorageUtil.getIntSizeInBytes(size, unit) * 2) / StorageUtil.BASE);
        } else if (SystemUtils.IS_OS_LINUX) {
            return mountLinuxRamDisk(name, size + unit.getLinuxUnitTypeInLetter());
        }
        throw new UnsupportedOperationException("Unsupported OS: " + System.getProperty("os.name"));
    }

    /**
     * Unmounts a disk
     *
     * @param name
     * @throws IOException
     * @throws InterruptedException
     */
    public static void unmountRamDisk(String name) throws IOException, InterruptedException {
        if (SystemUtils.IS_OS_MAC) {
            unmountMacRamDisk(name);
        } else if (SystemUtils.IS_OS_LINUX) {
            unmountLinuxRamDisk(name);
        }
    }

    private static Path mountMacRamDisk(String name, long size) throws IOException, InterruptedException {
        final String cmd = "diskutil erasevolume HFS+ '" + name + "' `hdiutil attach -nomount ram://" + size + "`";
        final ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", cmd);
        final Process p = pb.start();
        watchProcess(p);
        p.waitFor();
        return Paths.get("/Volumes", name);
    }

    private static void unmountMacRamDisk(String name) throws InterruptedException, IOException {
        final String cmd = "diskutil unmount " + name;
        final ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", cmd);
        final Process p = pb.start();
        watchProcess(p);
        p.waitFor();
    }

    private static Path mountLinuxRamDisk(String name, String size) throws IOException, InterruptedException {
        Path root = Paths.get("/tmp", name);
        if (!Files.exists(root)) {
            Files.createFile(root);
        }
        final String cmd = "mount -o size=" + size + " -t tmpfs none /tmp/" + name;
        final ProcessBuilder pb = new ProcessBuilder("bash", "-c", cmd);
        final Process p = pb.start();
        watchProcess(p);
        p.waitFor();
        return root;
    }

    private static void unmountLinuxRamDisk(String name) throws InterruptedException, IOException {
        final String cmd = "umount /tmp/" + name;
        final ProcessBuilder pb = new ProcessBuilder("bash", "-c", cmd);
        final Process p = pb.start();
        watchProcess(p);
        p.waitFor();
    }

    private static void watchProcess(Process p) {
        new Thread(() -> {
            final BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            try {
                while ((line = input.readLine()) != null) {
                    LOGGER.info(line);
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARN, e.getMessage(), e);
            }
        }).start();
    }
}
