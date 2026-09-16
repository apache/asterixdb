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
package org.apache.hyracks.api.network;

import java.net.InetSocketAddress;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.hyracks.util.annotations.AiProvenance;

public interface INetworkSecurityManager {

    /**
     * Creates a new ssl context based on the current configuration of this {@link INetworkSecurityManager}
     *
     * @return a new ssl context
     */
    SSLContext newSSLContext(boolean clientMode);

    /**
     * Creates a new server-mode ssl engine based on the current configuration of this
     * {@link INetworkSecurityManager}
     *
     * @return a new ssl engine
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "ASTERIXDB-3851")
    SSLEngine newServerSSLEngine();

    /**
     * Creates a new client-mode ssl engine based on the current configuration of this
     * {@link INetworkSecurityManager}. The engine verifies that the certificate the peer presents identifies
     * {@code peer}; validating the chain against the trust store on its own leaves any holder of a trusted
     * certificate able to answer in place of the node that was dialled.
     *
     * @param peer the address this connection was made to
     * @return a new ssl engine
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "ASTERIXDB-3851")
    SSLEngine newClientSSLEngine(InetSocketAddress peer);

    /**
     * Sets the configuration to be used for this {@link INetworkSecurityManager}
     *
     * @param config
     */
    void setConfiguration(INetworkSecurityConfig config);

    /**
     * Gets the socket channel factory
     *
     * @return the socket channel factory
     */
    ISocketChannelFactory getSocketChannelFactory();

    /**
     * Gets the current configuration of this {@link INetworkSecurityManager}
     *
     * @return the current configuration
     */
    INetworkSecurityConfig getConfiguration();
}
