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
package org.apache.asterix.external.api;

import java.util.Map;

import org.apache.asterix.common.parse.ITupleForwarder;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IDataFlowController {

    /**
     * Order of calls:
     * 1. Constructor()
     * 2. if record flow controller
     * |-a. Set record reader
     * |-b. Set record parser
     * else
     * |-a. Set stream parser
     * 3. setTupleForwarder(forwarder)
     * 4. configure(configuration,ctx)
     * 5. start(writer)
     *
     * pause(), resume(), and stop() are only used with feeds
     * pause is called after start when a feed is running and the system is overwhelmed with data.
     * resume is called after the load goes down and we are ready to receive more data.
     * stop is called to disconnect the feed. once stop is called, no other method is called.
     *
     */

    public void start(IFrameWriter writer) throws HyracksDataException;

    public boolean stop() throws HyracksDataException;

    public boolean pause() throws HyracksDataException;

    public boolean resume() throws HyracksDataException;

    public boolean handleException(Throwable th);

    public ITupleForwarder getTupleForwarder();

    public void setTupleForwarder(ITupleForwarder forwarder);

    public void configure(Map<String, String> configuration, IHyracksTaskContext ctx) throws HyracksDataException;
}
