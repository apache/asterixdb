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
package edu.uci.ics.hyracks.test.support;

import java.io.Serializable;
import java.util.concurrent.ThreadFactory;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.context.IHyracksRootContext;
import edu.uci.ics.hyracks.api.job.IJobSerializerDeserializerContainer;
import edu.uci.ics.hyracks.api.messages.IMessageBroker;
import edu.uci.ics.hyracks.api.resources.memory.IMemoryManager;

public class TestNCApplicationContext implements INCApplicationContext {
    private final IHyracksRootContext rootCtx;
    private final String nodeId;

    private Serializable distributedState;
    private Object appObject;

    private final IMemoryManager mm;

    public TestNCApplicationContext(IHyracksRootContext rootCtx, String nodeId) {
        this.rootCtx = rootCtx;
        this.nodeId = nodeId;
        mm = new IMemoryManager() {
            @Override
            public long getMaximumMemory() {
                return Long.MAX_VALUE;
            }

            @Override
            public long getAvailableMemory() {
                return Long.MAX_VALUE;
            }

            @Override
            public void deallocate(long memory) {

            }

            @Override
            public boolean allocate(long memory) {
                return true;
            }
        };
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    @Override
    public Serializable getDistributedState() {
        return distributedState;
    }

    @Override
    public IHyracksRootContext getRootContext() {
        return rootCtx;
    }

    @Override
    public void setApplicationObject(Object object) {
        this.appObject = object;
    }

    @Override
    public Object getApplicationObject() {
        return appObject;
    }

    @Override
    public void setMessageBroker(IMessageBroker staticticsConnector) {
        // TODO Auto-generated method stub

    }

    @Override
    public IMessageBroker getMessageBroker() {
        return null;
    }

	@Override
	public IJobSerializerDeserializerContainer getJobSerializerDeserializerContainer() {
		// TODO Auto-generated method stub
		return null;
	}

    @Override
    public IMemoryManager getMemoryManager() {
        return mm;
    }

    @Override
    public ThreadFactory getThreadFactory() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setThreadFactory(ThreadFactory threadFactory) {
        // TODO Auto-generated method stub

    }
}
