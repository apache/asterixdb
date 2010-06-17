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
package edu.uci.ics.hyracks.resources;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.context.HyracksContext;

public final class ResourceManager {
    private final HyracksContext ctx;

    public ResourceManager(HyracksContext ctx) {
        this.ctx = ctx;
    }

    public ByteBuffer allocateFrame() {
        return ByteBuffer.allocate(ctx.getFrameSize());
    }

    public File createFile(String prefix, String suffix) throws IOException {
        return File.createTempFile(prefix, suffix);
    }
}