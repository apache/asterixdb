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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.asterix.external.feed.dataflow.FeedFrameCollector.State;
import org.apache.asterix.external.feed.management.FeedConnectionId;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.log4j.Logger;

public class FrameDistributor implements IFrameWriter {

    public static final Logger LOGGER = Logger.getLogger(FrameDistributor.class.getName());
    /** A map storing the registered frame readers ({@code FeedFrameCollector}. **/
    private final Map<FeedConnectionId, FeedFrameCollector> registeredCollectors;
    private Throwable rootFailureCause = null;

    public FrameDistributor() throws HyracksDataException {
        this.registeredCollectors = new HashMap<FeedConnectionId, FeedFrameCollector>();
    }

    public synchronized void registerFrameCollector(FeedFrameCollector frameCollector) throws HyracksDataException {
        if (rootFailureCause != null) {
            throw new HyracksDataException("attempt to register to a failed feed data provider", rootFailureCause);
        }
        // registering a new collector.
        try {
            frameCollector.open();
        } catch (Throwable th) {
            rootFailureCause = th;
            try {
                frameCollector.fail();
            } catch (Throwable failThrowable) {
                th.addSuppressed(failThrowable);
            } finally {
                try {
                    frameCollector.close();
                } catch (Throwable closeThrowable) {
                    th.addSuppressed(closeThrowable);
                }
            }
            throw th;
        }
        registeredCollectors.put(frameCollector.getConnectionId(), frameCollector);
    }

    public synchronized void deregisterFrameCollector(FeedFrameCollector frameCollector) throws HyracksDataException {
        deregisterFrameCollector(frameCollector.getConnectionId());
    }

    public synchronized void deregisterFrameCollector(FeedConnectionId connectionId) throws HyracksDataException {
        if (rootFailureCause != null) {
            throw new HyracksDataException("attempt to register to a failed feed data provider", rootFailureCause);
        }
        FeedFrameCollector frameCollector = removeFrameCollector(connectionId);
        try {
            frameCollector.close();
        } catch (Throwable th) {
            rootFailureCause = th;
            throw th;
        }
    }

    public synchronized FeedFrameCollector removeFrameCollector(FeedConnectionId connectionId) {
        return registeredCollectors.remove(connectionId);
    }

    /*
     * Fix. What should be done?:
     * 0. mark failure so no one can subscribe or unsubscribe.
     * 1. Throw the throwable.
     * 2. when fail() is called, call fail on all subscribers
     * 3. close all the subscribers.
     * (non-Javadoc)
     * @see org.apache.hyracks.api.comm.IFrameWriter#nextFrame(java.nio.ByteBuffer)
     */
    @Override
    public synchronized void nextFrame(ByteBuffer frame) throws HyracksDataException {
        if (rootFailureCause != null) {
            throw new HyracksDataException(rootFailureCause);
        }
        for (FeedFrameCollector collector : registeredCollectors.values()) {
            try {
                collector.nextFrame(frame);
            } catch (Throwable th) {
                rootFailureCause = th;
                throw th;
            }
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        Collection<FeedFrameCollector> collectors = registeredCollectors.values();
        Iterator<FeedFrameCollector> it = collectors.iterator();
        while (it.hasNext()) {
            FeedFrameCollector collector = it.next();
            try {
                collector.fail();
            } catch (Throwable th) {
                while (it.hasNext()) {
                    FeedFrameCollector innerCollector = it.next();
                    try {
                        innerCollector.fail();
                    } catch (Throwable innerTh) {
                        th.addSuppressed(innerTh);
                    }
                }
                throw th;
            }
        }
    }

    @Override
    public void close() throws HyracksDataException {
        Collection<FeedFrameCollector> collectors = registeredCollectors.values();
        Iterator<FeedFrameCollector> it = collectors.iterator();
        while (it.hasNext()) {
            FeedFrameCollector collector = it.next();
            try {
                collector.close();
            } catch (Throwable th) {
                while (it.hasNext()) {
                    FeedFrameCollector innerCollector = it.next();
                    try {
                        innerCollector.close();
                    } catch (Throwable innerTh) {
                        th.addSuppressed(innerTh);
                    } finally {
                        innerCollector.setState(State.FINISHED);
                    }
                }
                // resume here
                throw th;
            } finally {
                collector.setState(State.FINISHED);
            }
        }
    }

    @Override
    public void flush() throws HyracksDataException {
        if (rootFailureCause != null) {
            throw new HyracksDataException(rootFailureCause);
        }
        for (FeedFrameCollector collector : registeredCollectors.values()) {
            try {
                collector.flush();
            } catch (Throwable th) {
                rootFailureCause = th;
                throw th;
            }
        }
    }

    @Override
    public void open() throws HyracksDataException {
        // Nothing to do here :)
    }
}
