/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.api.aqlj.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
import java.util.logging.Logger;

import edu.uci.ics.asterix.api.aqlj.common.AQLJProtocol;
import edu.uci.ics.asterix.api.aqlj.common.AQLJStream;

/**
 * This class handles data requests from the APIServer. When a query is executed
 * through the API, the output is written to the local disk of some NC. The
 * APIServer will contact that NC and ask for the results of the query to be
 * sent. This class handles such communication between the NC and APIServer.
 * 
 * @author zheilbron
 */
public class NodeDataClientThread extends Thread {
    private static final Logger LOGGER = Logger.getLogger(NodeDataClientThread.class.getName());

    private static final int RESULT_BUFFER_SIZE = 8192;

    private final AQLJStream aqljStream;

    public NodeDataClientThread(Socket clientSocket) throws IOException {
        aqljStream = new AQLJStream(clientSocket);
    }

    public void run() {
        try {
            getFile();
        } catch (IOException e) {
            LOGGER.severe("I/O error occurred over AQLJStream (socket)");
            LOGGER.severe(e.getMessage());
        } finally {
            close();
        }
    }

    private void getFile() throws IOException {
        aqljStream.receiveUnsignedInt32();
        int type = aqljStream.receiveChar();
        if ((char) type != AQLJProtocol.GET_RESULTS_MESSAGE) {
            return;
        }

        String path = aqljStream.receiveString();
        File outputFile = new File(path);
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(outputFile);
        } catch (FileNotFoundException e) {
            LOGGER.warning("Error: requested file not found: " + path);
            return;
        }

        byte[] buf = new byte[RESULT_BUFFER_SIZE];
        long maxPayload = 0xffffffffL - 5; // 2^32 (max size of payload) - 5
                                           // (header size)
        long remainingTotal = outputFile.length();
        long remainingInner = 0;
        int sentTotal = 0;
        int sentInner = 0;
        int toSend = 0;

        // the results may be large, so cram as much into a packet as possible
        while (remainingTotal > maxPayload) {
            aqljStream.sendUnsignedInt32(4 + 1 + maxPayload);
            aqljStream.sendChar(AQLJProtocol.DATA_MESSAGE);
            sentInner = 0;
            remainingInner = 0;
            while (sentInner < maxPayload) {
                remainingInner = maxPayload - sentInner;
                toSend = fis.read(buf, 0, (remainingInner > buf.length) ? buf.length : (int) remainingInner);
                sentInner += toSend;
                aqljStream.send(buf, 0, toSend);
            }
            aqljStream.flush();
            sentTotal += maxPayload;
            remainingTotal -= sentTotal;
        }

        // send the remaining data
        if (remainingTotal > 0) {
            aqljStream.sendUnsignedInt32(4 + 1 + (int) remainingTotal);
            aqljStream.sendChar(AQLJProtocol.DATA_MESSAGE);
            sentInner = 0;
            remainingInner = 0;
            while (sentInner < remainingTotal) {
                remainingInner = remainingTotal - sentInner;
                toSend = fis.read(buf, 0, (remainingInner > buf.length) ? buf.length : (int) remainingInner);
                sentInner += toSend;
                aqljStream.send(buf, 0, toSend);
            }
            aqljStream.flush();
        }
        outputFile.delete();
        aqljStream.sendUnsignedInt32(5);
        aqljStream.sendChar(AQLJProtocol.EXECUTE_COMPLETE_MESSAGE);
        aqljStream.flush();
    }

    private void close() {
        try {
            aqljStream.close();
        } catch (IOException e) {
            LOGGER.severe("Error closing AQLJStream");
            LOGGER.severe(e.getMessage());
        }
    }
}
