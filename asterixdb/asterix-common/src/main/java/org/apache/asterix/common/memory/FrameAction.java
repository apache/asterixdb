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
package org.apache.asterix.common.memory;

import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FrameAction {
    private static final boolean DEBUG = false;
    private static final Logger LOGGER = LogManager.getLogger();
    private ByteBuffer allocated;
    private ByteBuffer frame;

    public void call(ByteBuffer freeFrame) {
        if (DEBUG) {
            LOGGER.info("FrameAction: My subscription is being answered");
        }
        freeFrame.put(frame);
        synchronized (this) {
            allocated = freeFrame;
            if (DEBUG) {
                LOGGER.info("FrameAction: Waking up waiting threads");
            }
            notifyAll();
        }
    }

    public synchronized ByteBuffer retrieve() throws InterruptedException {
        if (DEBUG) {
            LOGGER.info("FrameAction: Attempting to get allocated buffer");
        }
        while (allocated == null) {
            if (DEBUG) {
                LOGGER.info("FrameAction: Allocated buffer is not ready yet. I will wait for it");
            }
            wait();
            if (DEBUG) {
                LOGGER.info("FrameAction: Awoken Up");
            }
        }
        ByteBuffer temp = allocated;
        allocated = null;
        return temp;
    }

    public void setFrame(ByteBuffer frame) {
        this.frame = frame;
    }

    public int getSize() {
        return frame.capacity();
    }
}
