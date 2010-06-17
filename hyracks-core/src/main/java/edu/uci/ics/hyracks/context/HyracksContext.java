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
package edu.uci.ics.hyracks.context;

import edu.uci.ics.hyracks.resources.ResourceManager;

public class HyracksContext {
    private final ResourceManager resourceManager;
    private final int frameSize;

    public HyracksContext(int frameSize) {
        resourceManager = new ResourceManager(this);
        this.frameSize = frameSize;
    }

    public ResourceManager getResourceManager() {
        return resourceManager;
    }

    public int getFrameSize() {
        return frameSize;
    }
}