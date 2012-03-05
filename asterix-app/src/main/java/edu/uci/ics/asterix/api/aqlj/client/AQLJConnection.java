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
package edu.uci.ics.asterix.api.aqlj.client;

import java.io.IOException;

import edu.uci.ics.asterix.api.aqlj.common.AQLJException;
import edu.uci.ics.asterix.api.aqlj.common.AQLJProtocol;
import edu.uci.ics.asterix.api.aqlj.common.AQLJStream;

/**
 * This class is the implementation of IAQLJConnection and is the means for
 * communication between a client and an ASTERIX server. The messages passed
 * through this connection conform to the AQLJ protocol.
 * 
 * @author zheilbron
 */
public class AQLJConnection implements IAQLJConnection {
    private final String dataverse;
    private final AQLJStream aqljStream;

    public AQLJConnection(String host, int port, String dataverse) throws AQLJException {
        this.dataverse = dataverse;

        try {
            aqljStream = new AQLJStream(host, port);
        } catch (IOException e) {
            throw new AQLJException("Could not connect to " + host + ":" + port);
        }

        startup();
    }

    private void startup() throws AQLJException {
        sendStartupMessage(dataverse);
        getStartupResponse();
    }

    private void sendStartupMessage(String dataverse) throws AQLJException {
        try {
            byte[] dvBytes = dataverse.getBytes("UTF-8");
            // 4 for the message length, 1 for the message type, 2 for the
            // string length
            aqljStream.sendUnsignedInt32(4 + 1 + 2 + dvBytes.length);
            aqljStream.sendChar(AQLJProtocol.STARTUP_MESSAGE);
            aqljStream.sendInt16(dvBytes.length);
            aqljStream.send(dvBytes);
            aqljStream.flush();
        } catch (IOException e) {
            throw new AQLJException(e);
        }
    }

    private void getStartupResponse() throws AQLJException {
        try {
            aqljStream.receiveUnsignedInt32();
            int messageType = aqljStream.receiveChar();
            switch (messageType) {
                case AQLJProtocol.READY_MESSAGE:
                    break;
                case AQLJProtocol.ERROR_MESSAGE:
                    String err = aqljStream.receiveString();
                    throw new AQLJException(err);
                default:
                    throw new AQLJException("Error: unable to parse message from server");
            }
        } catch (IOException e) {
            throw new AQLJException(e);
        }
    }

    @Override
    public IAQLJResult execute(String stmt) throws AQLJException {
        sendExecute(stmt);
        return fetchResults();
    }

    private AQLJResult fetchResults() throws AQLJException {
        long len;
        int messageType;

        ResultBuffer rb = null;
        while (true) {
            try {
                len = aqljStream.receiveUnsignedInt32();
                messageType = aqljStream.receiveChar();
                switch (messageType) {
                    case AQLJProtocol.DATA_MESSAGE:
                        // DataRecord
                        if (rb == null) {
                            rb = new ResultBuffer();
                        }
                        rb.appendMessage(aqljStream, (int) (len - 5));
                        break;
                    case AQLJProtocol.EXECUTE_COMPLETE_MESSAGE:
                        // ExecuteComplete
                        return new AQLJResult(rb);
                    case AQLJProtocol.ERROR_MESSAGE:
                        // Error
                        throw new AQLJException(aqljStream.receiveString());
                    default:
                        throw new AQLJException("Error: received unknown message type from server");
                }
            } catch (IOException e) {
                throw new AQLJException(e);
            }
        }

    }

    private void sendExecute(String stmt) throws AQLJException {
        try {
            byte[] stmtBytes = stmt.getBytes("UTF-8");
            // 4 for the message length, 1 for the message type, 2 for the
            // string length
            aqljStream.sendUnsignedInt32(4 + 1 + 2 + stmtBytes.length);
            aqljStream.sendChar(AQLJProtocol.EXECUTE_MESSAGE);
            aqljStream.sendInt16(stmtBytes.length);
            aqljStream.send(stmtBytes);
            aqljStream.flush();
        } catch (IOException e) {
            throw new AQLJException(e);
        }
    }

    @Override
    public void close() throws IOException {
        aqljStream.close();
    }

    @Override
    public IADMCursor createADMCursor() {
        return new ADMCursor(null);
    }
}
