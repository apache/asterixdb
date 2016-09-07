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
package org.apache.hyracks.api.comm;

public interface IChannelControlBlock {

    /**
     * Get the read interface of this channel.
     *
     * @return the read interface.
     */
    public IChannelReadInterface getReadInterface();

    /**
     * Get the write interface of this channel.
     *
     * @return the write interface.
     */
    public IChannelWriteInterface getWriteInterface();

    /**
     * Add write credit to this channel.
     *
     * @param delta
     *            number of bytes
     */
    public void addWriteCredits(int delta);

    /**
     * @return The channel's unique id within its ChannelSet.
     */
    public int getChannelId();

    /**
     * Add pending credit.
     *
     * @param credit
     */
    public void addPendingCredits(int credit);

    /**
     * Increments the pending write operations of this channel.
     */
    public void markPendingWrite();

    /**
     * Clears the pending write operations of this channel.
     */
    public void unmarkPendingWrite();

    /**
     * Sets a flag indicating this channel was closed locally.
     */
    public void reportLocalEOS();

    /**
     * A flag indicating if the channel was closed on the remote side.
     *
     * @return
     */
    public boolean isRemotelyClosed();

    /**
     * Complete the current write operation on this channel.
     */
    public void writeComplete();
}
