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
package edu.uci.ics.hyracks.ipc.api;

import java.util.concurrent.atomic.AtomicLong;

public class IPCPerformanceCounters {
    private final AtomicLong nMessagesSent;

    private final AtomicLong nMessageBytesSent;

    private final AtomicLong nMessagesReceived;

    private final AtomicLong nMessageBytesReceived;

    public IPCPerformanceCounters() {
        nMessagesSent = new AtomicLong();
        nMessageBytesSent = new AtomicLong();
        nMessagesReceived = new AtomicLong();
        nMessageBytesReceived = new AtomicLong();
    }

    public long getMessageSentCount() {
        return nMessagesSent.get();
    }

    public void addMessageSentCount(long delta) {
        nMessagesSent.addAndGet(delta);
    }

    public long getMessageBytesSent() {
        return nMessageBytesSent.get();
    }

    public void addMessageBytesSent(long delta) {
        nMessageBytesSent.addAndGet(delta);
    }

    public long getMessageReceivedCount() {
        return nMessagesReceived.get();
    }

    public void addMessageReceivedCount(long delta) {
        nMessagesReceived.addAndGet(delta);
    }

    public long getMessageBytesReceived() {
        return nMessageBytesReceived.get();
    }

    public void addMessageBytesReceived(long delta) {
        nMessageBytesReceived.addAndGet(delta);
    }
}