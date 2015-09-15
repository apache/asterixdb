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
package org.apache.asterix.tools.external.data;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.api.IFeedAdapter;
import org.apache.asterix.external.dataset.adapter.StreamBasedAdapter;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.dataflow.std.file.ITupleParserFactory;

public class GenericSocketFeedAdapter extends StreamBasedAdapter implements IFeedAdapter {

    private static final long serialVersionUID = 1L;

    private final int port;
    private SocketFeedServer socketFeedServer;

    public GenericSocketFeedAdapter(ITupleParserFactory parserFactory, ARecordType outputType, int port,
            IHyracksTaskContext ctx, int partition) throws AsterixException, IOException {
        super(parserFactory, outputType, ctx, partition);
        this.port = port;
        this.socketFeedServer = new SocketFeedServer(outputType, port);
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        super.start(partition, writer);
    }

    @Override
    public InputStream getInputStream(int partition) throws IOException {
        return socketFeedServer.getInputStream();
    }

    private static class SocketFeedServer {
        private ServerSocket serverSocket;
        private InputStream inputStream;

        public SocketFeedServer(ARecordType outputtype, int port) throws IOException, AsterixException {
            try {
                serverSocket = new ServerSocket(port);
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("port: " + port + " unusable ");
                }
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Feed server configured to use port: " + port);
            }
        }

        public InputStream getInputStream() {
            Socket socket;
            try {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("waiting for client at " + serverSocket.getLocalPort());
                }
                socket = serverSocket.accept();
                inputStream = socket.getInputStream();
            } catch (IOException e) {
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe("Unable to create input stream required for feed ingestion");
                }
            }
            return inputStream;
        }

        public void stop() throws IOException {
            try {
                serverSocket.close();
            } catch (IOException ioe) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Unable to close socket at " + serverSocket.getLocalPort());
                }
            }
        }

    }

    @Override
    public void stop() throws Exception {
        socketFeedServer.stop();
    }

    public DataExchangeMode getDataExchangeMode() {
        return DataExchangeMode.PUSH;
    }

    @Override
    public boolean handleException(Exception e) {
        try {
            this.socketFeedServer = new SocketFeedServer((ARecordType) sourceDatatype, port);
            return true;
        } catch (Exception re) {
            return false;
        }
    }

}
