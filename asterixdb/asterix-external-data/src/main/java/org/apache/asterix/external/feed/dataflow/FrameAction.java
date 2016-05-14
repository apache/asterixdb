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
package org.apache.asterix.external.feed.dataflow;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingDeque;

import rx.functions.Action1;

public class FrameAction implements Action1<ByteBuffer> {
    private final LinkedBlockingDeque<ByteBuffer> inbox;
    private ByteBuffer frame;

    public FrameAction(LinkedBlockingDeque<ByteBuffer> inbox) {
        this.inbox = inbox;
    }

    @Override
    public void call(ByteBuffer freeFrame) {
        freeFrame.put(frame);
        inbox.add(freeFrame);
        synchronized (this) {
            notify();
        }
    }

    public ByteBuffer getFrame() {
        return frame;
    }

    public void setFrame(ByteBuffer frame) {
        this.frame = frame;
    }

    public int getSize() {
        return frame.capacity();
    }
}