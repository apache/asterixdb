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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.input.stream.SocketServerInputStream;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class SocketServerInputStreamFactory implements IInputStreamFactory {

    private static final long serialVersionUID = 1L;
    private List<Pair<String, Integer>> sockets;
    private Mode mode = Mode.IP;

    public static enum Mode {
        NC,
        IP
    }

    @Override
    public void configure(IServiceContext serviceCtx, Map<String, String> configuration)
            throws AsterixException, CompilationException {
        try {
            sockets = new ArrayList<>();
            String modeValue = configuration.get(ExternalDataConstants.KEY_MODE);
            if (modeValue != null) {
                mode = Mode.valueOf(modeValue.trim().toUpperCase());
            }
            String socketsValue = configuration.get(ExternalDataConstants.KEY_SOCKETS);
            if (socketsValue == null) {
                throw new CompilationException(ErrorCode.FEED_METADATA_SOCKET_ADAPTOR_SOCKET_NOT_PROPERLY_CONFIGURED);
            }
            Map<InetAddress, Set<String>> ncMap;
            ncMap = RuntimeUtils.getNodeControllerMap((ICcApplicationContext) serviceCtx.getApplicationContext());
            List<String> ncs =
                    RuntimeUtils.getAllNodeControllers((ICcApplicationContext) serviceCtx.getApplicationContext());
            String[] socketsArray = socketsValue.split(",");
            Random random = new Random();
            for (String socket : socketsArray) {
                String[] socketTokens = socket.split(":");
                String host = socketTokens[0].trim();
                int port = Integer.parseInt(socketTokens[1].trim());
                Pair<String, Integer> p = null;
                switch (mode) {
                    case IP:
                        Set<String> ncsOnIp = ncMap.get(InetAddress.getByName(host));
                        if ((ncsOnIp == null) || ncsOnIp.isEmpty()) {
                            throw new CompilationException(
                                    ErrorCode.FEED_METADATA_SOCKET_ADAPTOR_SOCKET_INVALID_HOST_NC, "host", host,
                                    StringUtils.join(ncMap.keySet(), ", "));
                        }
                        String[] ncArray = ncsOnIp.toArray(new String[] {});
                        String nc = ncArray[random.nextInt(ncArray.length)];
                        p = new Pair<>(nc, port);
                        break;

                    case NC:
                        p = new Pair<>(host, port);
                        if (!ncs.contains(host)) {
                            throw new CompilationException(
                                    ErrorCode.FEED_METADATA_SOCKET_ADAPTOR_SOCKET_INVALID_HOST_NC, "NC", host,
                                    StringUtils.join(ncs, ", "));

                        }
                        break;
                }
                sockets.add(p);
            }
        } catch (CompilationException e) {
            throw e;
        } catch (HyracksDataException | UnknownHostException e) {
            throw new AsterixException(e);
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
            server.bind(new InetSocketAddress(socket.second));
            return new SocketServerInputStream(server);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() {
        List<String> locations = new ArrayList<>();
        for (Pair<String, Integer> socket : sockets) {
            locations.add(socket.first);
        }
        return new AlgebricksAbsolutePartitionConstraint(locations.toArray(new String[] {}));
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
