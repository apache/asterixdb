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
package org.apache.asterix.common.channels.api;

import java.io.IOException;
import java.util.List;

import org.apache.asterix.common.channels.ChannelId;

/**
 * Handle (de)registration of Channels for delivery of control messages.
 */
public interface IChannelConnectionManager {

    /**
     * Allows registration of a ChannelRuntime.
     * 
     * @param ChannelRuntime
     * @throws Exception
     */
    public void registerChannelRuntime(ChannelId channelId, IChannelRuntime channelRuntime) throws Exception;

    /**
     * Obtain Channel runtime corresponding to a ChannelId
     * 
     * @param ChannelId
     * @return
     */
    public IChannelRuntime getChannelRuntime(ChannelId ChannelId);

    /**
     * Allows de-registration of a Channel runtime.
     * 
     * @param ChannelId
     * @throws IOException
     */
    void deregisterChannelRuntime(ChannelId channelId) throws IOException;

    public List<ChannelId> getRegisteredRuntimes();

}
