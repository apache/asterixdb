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
package edu.uci.ics.hyracks.comm.channels;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.net.protocols.muxdemux.IBufferFactory;

/**
 * @author yingyib
 */
public class ReadBufferFactory implements IBufferFactory {

    private final int limit;
    private final int frameSize;
    private int counter = 0;

    public ReadBufferFactory(int limit, IHyracksCommonContext ctx) {
        this.limit = limit;
        this.frameSize = ctx.getFrameSize();
    }

    @Override
    public ByteBuffer createBuffer() {
        try {
            if (counter >= limit) {
                return null;
            } else {
                ByteBuffer frame = ByteBuffer.allocate(frameSize);
                counter++;
                return frame;
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}
