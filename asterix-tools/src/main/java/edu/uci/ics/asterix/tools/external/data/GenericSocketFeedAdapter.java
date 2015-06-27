package edu.uci.ics.asterix.tools.external.data;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.api.IFeedAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.StreamBasedAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

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
