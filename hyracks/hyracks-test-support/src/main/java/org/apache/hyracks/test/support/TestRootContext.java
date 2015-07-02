/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hyracks.test.support;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.context.IHyracksRootContext;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.control.nc.io.IOManager;

public class TestRootContext implements IHyracksRootContext {
    private IOManager ioManager;

    public TestRootContext() throws HyracksException {
        List<IODeviceHandle> devices = new ArrayList<IODeviceHandle>();
        devices.add(new IODeviceHandle(new File(System.getProperty("java.io.tmpdir")), "."));
        ioManager = new IOManager(devices, Executors.newCachedThreadPool());
    }

    @Override
    public IIOManager getIOManager() {
        return ioManager;
    }

    @Override
    public Map<String, NodeControllerInfo> getNodeControllerInfos() throws Exception {
        return null;
    }
}