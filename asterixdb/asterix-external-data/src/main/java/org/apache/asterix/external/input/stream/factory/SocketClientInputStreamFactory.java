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
package org.apache.asterix.external.input.stream.factory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.input.stream.SocketClientInputStream;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class SocketClientInputStreamFactory implements IInputStreamFactory {

    private static final long serialVersionUID = 1L;
    private transient IServiceContext serviceCtx;
    private transient AlgebricksAbsolutePartitionConstraint clusterLocations;
    private List<Pair<String, Integer>> sockets;

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() throws AlgebricksException {
        clusterLocations = IExternalDataSourceFactory.getPartitionConstraints(
                (ICcApplicationContext) serviceCtx.getApplicationContext(), clusterLocations, sockets.size());
        return clusterLocations;
    }

    @Override
    public void configure(IServiceContext serviceCtx, Map<String, String> configuration) throws AsterixException {
        try {
            this.serviceCtx = serviceCtx;
            this.sockets = new ArrayList<>();
            String socketsValue = configuration.get(ExternalDataConstants.KEY_SOCKETS);
            if (socketsValue == null) {
                throw new IllegalArgumentException(
                        "\'sockets\' parameter not specified as part of adapter configuration");
            }
            String[] socketsArray = socketsValue.split(",");
            for (String socket : socketsArray) {
                String[] socketTokens = socket.split(":");
                String host = socketTokens[0].trim();
                int port = Integer.parseInt(socketTokens[1].trim());
                InetAddress[] resolved;
                resolved = SystemDefaultDnsResolver.INSTANCE.resolve(host);
                Pair<String, Integer> p = new Pair<>(resolved[0].getHostAddress(), port);
                sockets.add(p);
            }
        } catch (UnknownHostException e) {
            throw new AsterixException(e);
        }
    }

    @Override
    public AsterixInputStream createInputStream(IHyracksTaskContext ctx, int partition) throws HyracksDataException {
        try {
            return new SocketClientInputStream(sockets.get(partition));
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
