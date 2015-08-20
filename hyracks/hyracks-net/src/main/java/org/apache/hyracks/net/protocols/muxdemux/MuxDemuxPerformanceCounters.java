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
package edu.uci.ics.hyracks.net.protocols.muxdemux;

import java.util.concurrent.atomic.AtomicLong;

public class MuxDemuxPerformanceCounters {
    private final AtomicLong payloadBytesRead;

    private final AtomicLong payloadBytesWritten;

    private final AtomicLong signalingBytesRead;

    private final AtomicLong signalingBytesWritten;

    public MuxDemuxPerformanceCounters() {
        payloadBytesRead = new AtomicLong();
        payloadBytesWritten = new AtomicLong();
        signalingBytesRead = new AtomicLong();
        signalingBytesWritten = new AtomicLong();
    }

    public void addPayloadBytesRead(long delta) {
        payloadBytesRead.addAndGet(delta);
    }

    public long getPayloadBytesRead() {
        return payloadBytesRead.get();
    }

    public void addPayloadBytesWritten(long delta) {
        payloadBytesWritten.addAndGet(delta);
    }

    public long getPayloadBytesWritten() {
        return payloadBytesWritten.get();
    }

    public void addSignalingBytesRead(long delta) {
        signalingBytesRead.addAndGet(delta);
    }

    public long getSignalingBytesRead() {
        return signalingBytesRead.get();
    }

    public void addSignalingBytesWritten(long delta) {
        signalingBytesWritten.addAndGet(delta);
    }

    public long getSignalingBytesWritten() {
        return signalingBytesWritten.get();
    }
}