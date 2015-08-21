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
package edu.uci.ics.asterix.tools.external.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.api.IFeedAdapter;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class SocketClientAdapter implements IFeedAdapter {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(SocketClientAdapter.class.getName());

    private static final String LOCALHOST = "127.0.0.1";

    private static final long RECONNECT_PERIOD = 2000;

    private final String localFile;

    private final int port;

    private final IHyracksTaskContext ctx;

    private boolean continueStreaming = true;

    public SocketClientAdapter(Integer port, String localFile, IHyracksTaskContext ctx) {
        this.localFile = localFile;
        this.port = port;
        this.ctx = ctx;
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        Socket socket = waitForReceiver();
        OutputStream os = socket.getOutputStream();
        FileInputStream fin = new FileInputStream(new File(localFile));
        byte[] chunk = new byte[1024];
        int read;
        try {
            while (continueStreaming) {
                read = fin.read(chunk);
                if (read > 0) {
                    os.write(chunk, 0, read);
                } else {
                    break;
                }
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Finished streaming file " + localFile + "to port [" + port + "]");
            }

        } finally {
            socket.close();
            fin.close();
        }

    }

    private Socket waitForReceiver() throws Exception {
        Socket socket = null;
        while (socket == null) {
            try {
                socket = new Socket(LOCALHOST, port);
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Receiver not ready, would wait for " + (RECONNECT_PERIOD / 1000)
                            + " seconds before reconnecting");
                }
                Thread.sleep(RECONNECT_PERIOD);
            }
        }
        return socket;
    }

    @Override
    public DataExchangeMode getDataExchangeMode() {
        return DataExchangeMode.PUSH;
    }

    @Override
    public void stop() throws Exception {
        continueStreaming = false;
    }

    @Override
    public boolean handleException(Exception e) {
        return false;
    }

}
