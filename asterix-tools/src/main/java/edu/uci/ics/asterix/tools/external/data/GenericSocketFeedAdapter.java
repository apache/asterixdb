package edu.uci.ics.asterix.tools.external.data;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.dataset.adapter.StreamBasedAdapter;
import edu.uci.ics.asterix.metadata.feeds.IFeedAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class GenericSocketFeedAdapter extends StreamBasedAdapter implements IFeedAdapter {

    private static final long serialVersionUID = 1L;

    public static final String KEY_PORT = "port";

    private static final Logger LOGGER = Logger.getLogger(GenericSocketFeedAdapter.class.getName());

    private Map<String, String> configuration;

    private SocketFeedServer socketFeedServer;

    private static final int DEFAULT_PORT = 2909;

    public GenericSocketFeedAdapter(Map<String, String> configuration, ITupleParserFactory parserFactory,
            ARecordType outputtype, IHyracksTaskContext ctx) throws AsterixException, IOException {
        super(parserFactory, outputtype, ctx);
        this.configuration = configuration;
        String portValue = (String) this.configuration.get(KEY_PORT);
        int port = portValue != null ? Integer.parseInt(portValue) : DEFAULT_PORT;
        this.socketFeedServer = new SocketFeedServer(configuration, outputtype, port);
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

        public SocketFeedServer(Map<String, String> configuration, ARecordType outputtype, int port)
                throws IOException, AsterixException {
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
                socket = serverSocket.accept();
                inputStream = socket.getInputStream();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return inputStream;
        }

        public void stop() throws IOException {
            serverSocket.close();
        }

    }

    @Override
    public void stop() throws Exception {
        socketFeedServer.stop();
    }

    @Override
    public void alter(Map<String, String> properties) {
        // TODO Auto-generated method stub

    }
}
