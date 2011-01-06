/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.test.support;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import edu.uci.ics.hyracks.api.context.IHyracksRootContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.control.nc.io.IOManager;

public class TestRootContext implements IHyracksRootContext {
    private int frameSize;
    private IOManager ioManager;

    public TestRootContext(int frameSize) throws HyracksException {
        this.frameSize = frameSize;
        List<IODeviceHandle> devices = new ArrayList<IODeviceHandle>();
        devices.add(new IODeviceHandle(new File(System.getProperty("java.io.tmpdir")), "."));
        ioManager = new IOManager(devices, Executors.newCachedThreadPool());
    }

    @Override
    public ByteBuffer allocateFrame() {
        return ByteBuffer.allocate(frameSize);
    }

    @Override
    public int getFrameSize() {
        return frameSize;
    }

    @Override
    public IIOManager getIOManager() {
        return ioManager;
    }
}