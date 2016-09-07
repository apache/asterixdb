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
package org.apache.hyracks.net.protocols.muxdemux;

import org.apache.hyracks.api.comm.IChannelControlBlock;
import org.apache.hyracks.api.comm.IChannelInterfaceFactory;
import org.apache.hyracks.api.comm.IChannelReadInterface;
import org.apache.hyracks.api.comm.IChannelWriteInterface;

public class FullFrameChannelInterfaceFactory implements IChannelInterfaceFactory {

    public static final IChannelInterfaceFactory INSTANCE = new FullFrameChannelInterfaceFactory();

    @Override
    public IChannelReadInterface createReadInterface(IChannelControlBlock cbb) {
        return new FullFrameChannelReadInterface(cbb);
    }

    @Override
    public IChannelWriteInterface createWriteInterface(IChannelControlBlock cbb) {
        return new FullFrameChannelWriteInterface(cbb);
    }
}
