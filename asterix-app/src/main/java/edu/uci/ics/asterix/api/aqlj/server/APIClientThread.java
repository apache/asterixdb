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
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.uci.ics.asterix.api.aqlj.common.AQLJException;
import edu.uci.ics.asterix.api.aqlj.common.AQLJProtocol;
import edu.uci.ics.asterix.api.aqlj.common.AQLJStream;
import edu.uci.ics.asterix.api.common.APIFramework.DisplayFormat;
import edu.uci.ics.asterix.api.common.AsterixHyracksIntegrationUtil;
import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.parser.ParseException;
import edu.uci.ics.asterix.aql.translator.AqlTranslator;
import edu.uci.ics.asterix.aql.translator.QueryResult;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.hyracks.bootstrap.AsterixNodeState;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.api.IAsterixStateProxy;
import edu.uci.ics.asterix.metadata.bootstrap.AsterixProperties;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;

/**
 * This class is the client handler for the APIServer. The AQLJ protocol is used
 * for communicating with the client. The client, for example, may send a
 * message to execute a set containing one or more AQL statements. It is up to this class to process each
 * AQL statement (in the original order) and pass back the results, if any, to the client.
 * 
 * @author zheilbron
 */
public class APIClientThread extends Thread {
    private static final Logger LOGGER = Logger.getLogger(APIClientThread.class.getName());

    private static final int RESULT_BUF_SIZE = 8192;

    private final IHyracksClientConnection hcc;
    private final ICCApplicationContext appContext;
    private final AQLJStream clientStream;
    private final String outputFilePath;
    private final String outputNodeName;
    private final String outputNodeIP;
    private final String binaryOutputClause;

    private AQLJStream nodeDataServerStream;
    private int nodeDataServerPort;
    private String dataverse;

    public APIClientThread(IHyracksClientConnection hcc, Socket clientSocket, ICCApplicationContext appCtx)
            throws IOException {
        this.hcc = hcc;
        clientStream = new AQLJStream(clientSocket);
        this.appContext = appCtx;

        // get the name of the first node controller that we find
        // all query results will be written to this node
        Map<String, Set<String>> nodeNameMap = new HashMap<String, Set<String>>();
        try {
            this.appContext.getCCContext().getIPAddressNodeMap(nodeNameMap);
        } catch (Exception e) {
            throw new IOException(" unable to obtain IP address node map", e);
        }
        outputNodeIP = (String) nodeNameMap.keySet().toArray()[0];
        outputNodeName = (String) nodeNameMap.get(outputNodeIP).toArray()[0];

        // get the port of the node data server that is running on the first nc
        IAsterixStateProxy proxy = (IAsterixStateProxy) appCtx.getDistributedState();
        nodeDataServerPort = ((AsterixNodeState) proxy.getAsterixNodeState(outputNodeName)).getAPINodeDataServerPort();
        nodeDataServerStream = null;

        // write the data into the output stores directory of the nc
        // if output stores are unavailable (could they ever be?), then write to
        // tmpdir which can be overridden
        // Also, use milliseconds in path name of output file to differentiate
        // queries
        Map<String, String[]> storesMap = AsterixProperties.INSTANCE.getStores();
        String[] outputStores = storesMap.get(outputNodeName);
        if (outputStores.length > 0) {
            outputFilePath = outputStores[0] + System.currentTimeMillis() + ".adm";
        } else {
            outputFilePath = System.getProperty("java.io.tmpdir") + File.pathSeparator + System.currentTimeMillis()
                    + ".adm";
        }

        // the "write output..." clause is inserted into incoming AQL statements
        binaryOutputClause = "write output to " + outputNodeName + ":\"" + outputFilePath
                + "\" using \"edu.uci.ics.hyracks.algebricks.runtime.writers.SerializedDataWriterFactory\";";

    }

    private void startup() throws IOException {
        int messageType;

        clientStream.receiveUnsignedInt32();
        messageType = clientStream.receiveChar();
        dataverse = clientStream.receiveString();
        if (messageType == AQLJProtocol.STARTUP_MESSAGE) {
            // send Ready
            sendReady();
        } else {
            // send Error
            LOGGER.warning("Error: received message other than Startup. Exiting.");
            String err = "startup failed: no Startup message received";
            sendError(err);
        }
    }

    public void run() {
        String outputPath;
        int messageType;

        try {
            // startup phase
            startup();

            // normal execution phase
            while (true) {
                // check if we should close
                if (Thread.interrupted()) {
                    close();
                    return;
                }

                clientStream.receiveUnsignedInt32();
                messageType = clientStream.receiveChar();
                switch (messageType) {
                    case AQLJProtocol.EXECUTE_MESSAGE:
                        // Execute
                        String query = clientStream.receiveString();
                        String fullQuery = "use dataverse " + dataverse + ";\n" + binaryOutputClause + '\n' + query;

                        try {
                            outputPath = executeStatement(fullQuery);
                        } catch (AQLJException e) {
                            LOGGER.severe("Error occurred while executing query: " + fullQuery);
                            LOGGER.severe(e.getMessage());
                            sendError(e.getMessage());
                            break;
                        }

                        if (outputPath == null) {
                            // The query ran, but produced no results. This
                            // means cardinality of the
                            // result is 0 or "actions" were performed, where
                            // actions are things like create
                            // type, create dataset, etc.
                            sendExecuteComplete();
                        } else {
                            // otherwise, there are some results, so send them
                            // back to the client
                            if (sendResults(outputPath)) {
                                sendExecuteComplete();
                            } else {
                                String err = "Error: unable to retrieve results from " + outputNodeName;
                                LOGGER.severe(err);
                                sendError(err);
                            }
                        }
                        break;
                    default:
                        String err = "Error: received unknown message of type " + (char) messageType;
                        sendError(err);
                        LOGGER.severe(err);
                        close();
                        return;
                }
            }
        } catch (IOException e) {
            // the normal path that is taken when exiting
            close();
            return;
        }
    }

    private void close() {
        try {
            if (nodeDataServerStream != null) {
                nodeDataServerStream.close();
            }
        } catch (IOException e) {
            LOGGER.severe("Error closing NodeData AQLJStream");
            LOGGER.severe(e.getMessage());
        }
        try {
            clientStream.close();
        } catch (IOException e) {
            LOGGER.severe("Error closing client AQLJStream");
            LOGGER.severe(e.getMessage());
        }
    }

    private String executeStatement(String stmt) throws IOException, AQLJException {
        List<QueryResult> executionResults = null;
        PrintWriter out = new PrintWriter(System.out);
        try {
            AQLParser parser = new AQLParser(new StringReader(stmt));
            List<Statement> statements = parser.Statement();
            SessionConfig pc = new SessionConfig(AsterixHyracksIntegrationUtil.DEFAULT_HYRACKS_CC_CLIENT_PORT, true,
                    false, false, false, false, false, true, false);

            MetadataManager.INSTANCE.init();
            if (statements != null && statements.size() > 0) {
                AqlTranslator translator = new AqlTranslator(statements, out, pc, DisplayFormat.TEXT);
                executionResults = translator.compileAndExecute(hcc);
            }
        } catch (ParseException e) {
            e.printStackTrace();
            throw new AQLJException(e);
        } catch (AsterixException e) {
            e.printStackTrace();
            throw new AQLJException(e);
        } catch (AlgebricksException e) {
            e.printStackTrace();
            throw new AQLJException(e);
        } catch (Exception e) {
            e.printStackTrace();
            sendError(e.getMessage());
        }
        return executionResults.get(0).getResultPath();

    }

    private boolean sendResults(String path) throws IOException {
        int messageType;
        long len;
        int sent;
        int toSend;
        byte[] buf = new byte[RESULT_BUF_SIZE];

        if (nodeDataServerStream == null) {
            nodeDataServerStream = new AQLJStream(outputNodeIP, nodeDataServerPort);
        }
        sendGetResults(nodeDataServerStream);

        // forward data packets from the nodedataservers through this server to
        // the client
        while (true) {
            len = nodeDataServerStream.receiveUnsignedInt32();
            messageType = nodeDataServerStream.receiveChar();
            switch ((char) messageType) {
                case AQLJProtocol.DATA_MESSAGE:
                    clientStream.sendUnsignedInt32(len);
                    clientStream.sendChar(AQLJProtocol.DATA_MESSAGE);
                    len -= 5;
                    sent = 0;
                    while (sent < len) {
                        len -= sent;
                        toSend = (len > buf.length) ? buf.length : (int) len;
                        nodeDataServerStream.receive(buf, 0, toSend);
                        clientStream.send(buf, 0, toSend);
                        sent += toSend;
                    }
                    clientStream.flush();
                    break;
                case AQLJProtocol.EXECUTE_COMPLETE_MESSAGE:
                    nodeDataServerStream.close();
                    nodeDataServerStream = null;
                    return true;
                default:
                    nodeDataServerStream.close();
                    nodeDataServerStream = null;
                    return false;
            }
        }
    }

    private void sendGetResults(AQLJStream s) throws IOException {
        byte[] pathBytes = outputFilePath.getBytes("UTF-8");
        // 4 for the message length, 1 for the message type, 2 for the string
        // length
        s.sendUnsignedInt32(4 + 1 + 2 + pathBytes.length);
        s.sendChar(AQLJProtocol.GET_RESULTS_MESSAGE);
        s.sendString(outputFilePath);
        s.flush();
    }

    private void sendReady() throws IOException {
        // 4 for the message length and 1 for the message type (4 + 1 = 5)
        clientStream.sendUnsignedInt32(5);
        clientStream.sendChar(AQLJProtocol.READY_MESSAGE);
        clientStream.flush();
    }

    private void sendError(String msg) throws IOException {
        byte[] msgBytes = msg.getBytes("UTF-8");
        // 4 for the message length, 1 for the message type, 2 for the string
        // length
        clientStream.sendUnsignedInt32(4 + 1 + 2 + msgBytes.length);
        clientStream.sendChar(AQLJProtocol.ERROR_MESSAGE);
        clientStream.sendInt16(msgBytes.length);
        clientStream.send(msgBytes);
        clientStream.flush();
    }

    private void sendExecuteComplete() throws IOException {
        // 4 for the message length and 1 for the message type (4 + 1 = 5)
        clientStream.sendUnsignedInt32(5);
        clientStream.sendChar(AQLJProtocol.EXECUTE_COMPLETE_MESSAGE);
        clientStream.flush();
    }
}
