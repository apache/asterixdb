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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.input.stream.SocketServerInputStream;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class SocketServerInputStreamFactory implements IInputStreamFactory {

    private static final long serialVersionUID = 1L;
    private List<Pair<String, Integer>> sockets;

    @Override
    public void configure(IServiceContext serviceCtx, Map<String, String> configuration) throws CompilationException {
        try {
            sockets = FeedUtils.extractHostsPorts(configuration.get(ExternalDataConstants.KEY_MODE), serviceCtx,
                    configuration.get(ExternalDataConstants.KEY_SOCKETS));
        } catch (CompilationException e) {
            throw e;
        } catch (Exception e) {
            throw new CompilationException(ErrorCode.FEED_METADATA_SOCKET_ADAPTOR_SOCKET_NOT_PROPERLY_CONFIGURED);
        }
    }

    @Override
    public synchronized AsterixInputStream createInputStream(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        try {
            Pair<String, Integer> socket = sockets.get(partition);
            ServerSocket server;
            server = new ServerSocket();
            server.bind(new InetSocketAddress(socket.getRight()));
            return new SocketServerInputStream(server);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() {
        return FeedUtils.addressToAbsolutePartitionConstraints(sockets);
    }

    public List<Pair<String, Integer>> getSockets() {
        return sockets;
    }

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.STREAM;
    }

    @Override
    public boolean isIndexible() {
        return false;
    }
}
