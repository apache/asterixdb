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
package org.apache.hyracks.control.nc.io;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IFileDeviceComputer;
import org.apache.hyracks.api.io.IODeviceHandle;

public class DefaultDeviceComputer implements IFileDeviceComputer {
    private final IOManager ioManager;
    private AtomicInteger next = new AtomicInteger(0);

    public DefaultDeviceComputer(IOManager ioManager) {
        this.ioManager = ioManager;
    }

    @Override
    public IODeviceHandle compute(String relPath) throws HyracksDataException {
        String path = relPath;
        // if number of devices is 1, we return the device
        if (ioManager.getIODevices().size() == 1) {
            return ioManager.getIODevices().get(0);
        }
        // check if it exists already on a device
        int nextSeparator = path.lastIndexOf(File.separator);
        while (nextSeparator > 0) {
            for (IODeviceHandle dev : ioManager.getIODevices()) {
                if (dev.contains(path)) {
                    return dev;
                }
            }
            path = path.substring(0, nextSeparator);
            nextSeparator = path.lastIndexOf(File.separator);
        }
        // one last attempt
        for (IODeviceHandle dev : ioManager.getIODevices()) {
            if (dev.contains(path)) {
                return dev;
            }
        }
        // not on any device, round robin assignment
        return ioManager.getIODevices().get(next.getAndIncrement() % ioManager.getIODevices().size());
    }

}
