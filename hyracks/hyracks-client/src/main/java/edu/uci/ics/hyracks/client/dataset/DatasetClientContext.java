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
package edu.uci.ics.hyracks.client.dataset;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.io.IIOManager;

public class DatasetClientContext implements IHyracksCommonContext {
    private final int frameSize;

    public DatasetClientContext(int frameSize) {
        this.frameSize = frameSize;
    }

    @Override
    public int getFrameSize() {
        return frameSize;
    }

    @Override
    public IIOManager getIOManager() {
        return null;
    }

    @Override
    public ByteBuffer allocateFrame() {
        return ByteBuffer.allocate(frameSize);
    }

    @Override
    public void deallocateFrames(int frameCount) {
        // TODO Auto-generated method stub
        
    }

}
