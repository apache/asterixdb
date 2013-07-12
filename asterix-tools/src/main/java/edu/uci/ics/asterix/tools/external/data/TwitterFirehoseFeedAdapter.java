package edu.uci.ics.asterix.tools.external.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.dataset.adapter.IPullBasedFeedClient.InflowState;
import edu.uci.ics.asterix.external.dataset.adapter.StreamBasedAdapter;
import edu.uci.ics.asterix.metadata.feeds.IFeedAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

/**
 * TPS can be configured between 1 and 20,000
 * 
 * @author ramang
 */
public class TwitterFirehoseFeedAdapter extends StreamBasedAdapter implements IFeedAdapter {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(TwitterFirehoseFeedAdapter.class.getName());

    private final TwitterServer twitterServer;

    private TwitterClient twitterClient;

    private static final String LOCALHOST = "127.0.0.1";
    private static final int PORT = 2909;

    public TwitterFirehoseFeedAdapter(Map<String, String> configuration, ITupleParserFactory parserFactory,
            ARecordType outputtype, IHyracksTaskContext ctx) throws AsterixException, IOException {
        super(parserFactory, outputtype, ctx);
        this.twitterServer = new TwitterServer(configuration, outputtype);
        this.twitterClient = new TwitterClient(twitterServer.getPort());
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        twitterServer.start();
        twitterClient.start();
        super.start(partition, writer);
    }

    @Override
    public InputStream getInputStream(int partition) throws IOException {
        return twitterClient.getInputStream();
    }

    private static class TwitterServer {
        private ServerSocket serverSocket;
        private final Listener listener;
        private int port = -1;

        public TwitterServer(Map<String, String> configuration, ARecordType outputtype) throws IOException,
                AsterixException {
            int numAttempts = 0;
            while (port < 0) {
                try {
                    serverSocket = new ServerSocket(PORT + numAttempts);
                    port = PORT + numAttempts;
                } catch (Exception e) {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("port: " + (PORT + numAttempts) + " unusable ");
                    }
                    numAttempts++;
                }
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Twitter server configured to use port: " + port);
            }
            listener = new Listener(serverSocket, configuration, outputtype);
        }

        public void start() {
            Thread t = new Thread(listener);
            t.start();
        }

        public void stop() {
            listener.stop();
        }

        public int getPort() {
            return port;
        }

    }

    private static class TwitterClient {

        private Socket socket;
        private int port;

        public TwitterClient(int port) throws UnknownHostException, IOException {
            this.port = port;
        }

        public InputStream getInputStream() throws IOException {
            return socket.getInputStream();
        }

        public void start() throws UnknownHostException, IOException {
            socket = new Socket(LOCALHOST, port);
        }
    }

    private static class Listener implements Runnable {

        private final ServerSocket serverSocket;
        private Socket socket;
        private TweetGenerator tweetGenerator;
        private boolean continuePush = true;

        public Listener(ServerSocket serverSocket, Map<String, String> configuration, ARecordType outputtype)
                throws IOException, AsterixException {
            this.serverSocket = serverSocket;
            this.tweetGenerator = new TweetGenerator(configuration, outputtype, 0,
                    TweetGenerator.OUTPUT_FORMAT_ADM_STRING);
        }

        @Override
        public void run() {
            while (true) {
                InflowState state = InflowState.DATA_AVAILABLE;
                try {
                    socket = serverSocket.accept();
                    OutputStream os = socket.getOutputStream();
                    tweetGenerator.setOutputStream(os);
                    while (state.equals(InflowState.DATA_AVAILABLE) && continuePush) {
                        state = tweetGenerator.setNextRecord();
                    }
                    os.close();
                    break;
                } catch (Exception e) {

                } finally {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            }
        }

        public void stop() {
            continuePush = false;
        }

    }

    @Override
    public void stop() throws Exception {
        twitterServer.stop();
    }

    @Override
    public void alter(Map<String, String> properties) {
        // TODO Auto-generated method stub

    }
}
