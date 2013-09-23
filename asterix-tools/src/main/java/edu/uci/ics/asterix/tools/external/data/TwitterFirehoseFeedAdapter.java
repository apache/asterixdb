package edu.uci.ics.asterix.tools.external.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.external.dataset.adapter.StreamBasedAdapter;
import edu.uci.ics.asterix.metadata.feeds.IFeedAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

/**
 * TPS can be configured between 1 and 20,000
 * 
 */
public class TwitterFirehoseFeedAdapter extends StreamBasedAdapter implements IFeedAdapter {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(TwitterFirehoseFeedAdapter.class.getName());

    private final TwitterServer twitterServer;

    private TwitterClient twitterClient;

    private static final String LOCALHOST = "127.0.0.1";
    private static final int PORT = 2909;

    private ExecutorService executorService = Executors.newCachedThreadPool();

    public TwitterFirehoseFeedAdapter(Map<String, String> configuration, ITupleParserFactory parserFactory,
            ARecordType outputtype, IHyracksTaskContext ctx, int partition) throws Exception {
        super(parserFactory, outputtype, ctx);
        this.twitterServer = new TwitterServer(configuration, outputtype, executorService, partition);
        this.twitterClient = new TwitterClient(twitterServer.getPort());
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        twitterServer.start();
        twitterServer.getListener().setPartition(partition);
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
        private ExecutorService executorService;

        public TwitterServer(Map<String, String> configuration, ARecordType outputtype, ExecutorService executorService, int partition)
                throws Exception {
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
            String dvds = configuration.get("dataverse-dataset");
            listener = new Listener(serverSocket, configuration, outputtype, dvds, partition);
            this.executorService = executorService;
        }

        public Listener getListener() {
            return listener;
        }

        public void start() {
            executorService.execute(listener);
        }

        public void stop() throws IOException {
            listener.stop();
            serverSocket.close();
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
        private TweetGenerator2 tweetGenerator;
        private boolean continuePush = true;
        private int fixedTps = -1;
        private int minTps = -1;
        private int maxTps = -1;
        private int tputDuration;
        private int partition;
        private Rate task;
        private Mode mode;

        public static final String KEY_MODE = "mode";

        public static enum Mode {
            AGGRESSIVE,
            CONTROLLED,
        }

        public void setPartition(int partition) {
            this.partition = partition;
            task.setPartition(partition);
        }

        public Listener(ServerSocket serverSocket, Map<String, String> configuration, ARecordType outputtype,
                String datasetName, int partition) throws Exception {
            this.serverSocket = serverSocket;
            this.tweetGenerator = new TweetGenerator2(configuration, partition, TweetGenerator.OUTPUT_FORMAT_ADM_STRING);
            String value = configuration.get(KEY_MODE);
            String confValue = null;
            if (value != null) {
                mode = Mode.valueOf(value.toUpperCase());
                switch (mode) {
                    case AGGRESSIVE:
                        break;
                    case CONTROLLED:
                        confValue = configuration.get(TweetGenerator2.KEY_TPS);
                        if (confValue != null) {
                            minTps = Integer.parseInt(confValue);
                            maxTps = minTps;
                            fixedTps = minTps;
                        } else {
                            confValue = configuration.get(TweetGenerator2.KEY_MIN_TPS);
                            if (confValue != null) {
                                minTps = Integer.parseInt(confValue);
                            }
                            confValue = configuration.get(TweetGenerator2.KEY_MAX_TPS);
                            if (confValue != null) {
                                maxTps = Integer.parseInt(configuration.get(TweetGenerator2.KEY_MAX_TPS));
                            }

                            if (minTps < 0 || maxTps < 0 || minTps > maxTps) {
                                throw new IllegalArgumentException("Incorrect value for min/max TPS");
                            }
                        }

                }
            } else {
                mode = Mode.AGGRESSIVE;
            }

            tputDuration = Integer.parseInt(configuration.get(TweetGenerator2.KEY_TPUT_DURATION));
            task = new Rate(tweetGenerator, tputDuration, datasetName, partition);

        }

        @Override
        public void run() {
            while (true) {
                try {
                    socket = serverSocket.accept();
                    OutputStream os = socket.getOutputStream();
                    tweetGenerator.setOutputStream(os);
                    boolean moreData = true;
                    Timer timer = new Timer();
                    timer.schedule(task, tputDuration * 1000, tputDuration * 1000);
                    long startBatch;
                    long endBatch;
                    Random random = new Random();
                    int tps = 0;
                    while (moreData && continuePush) {
                        if(maxTps > 0){
                             tps = minTps + random.nextInt((maxTps+1) - minTps);   
                        } else {
                            tps = fixedTps;
                        }
                        startBatch = System.currentTimeMillis();
                        moreData = tweetGenerator.setNextRecordBatch(tps);
                        endBatch = System.currentTimeMillis();
                        if (mode.equals(Mode.CONTROLLED)) {
                            if (endBatch - startBatch < 1000) {
                                Thread.sleep(1000 - (endBatch - startBatch));
                            }
                        }
                    }
                    timer.cancel();
                    os.close();
                    break;
                } catch (Exception e) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Exception in adaptor " + e.getMessage());
                    }
                } finally {
                    try {
                        if (socket != null && socket.isClosed()) {
                            socket.close();
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("Closed socket:" + socket.getPort());
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            }
        }

        public void stop() {
            continuePush = false;
        }

        private static class Rate extends TimerTask {

            private TweetGenerator2 gen;
            int prevMeasuredTweets = 0;
            private int tputDuration;
            private int partition;
            private String dataset;

            public Rate(TweetGenerator2 gen, int tputDuration, String dataset, int partition) {
                this.gen = gen;
                this.tputDuration = tputDuration;
                this.dataset = dataset;
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning(new Date() + " " + "Dataset" + " " + "partition" + " " + "Total flushed tweets"
                            + "\t" + "intantaneous throughput");
                }
            }

            @Override
            public void run() {

                int currentMeasureTweets = gen.getNumFlushedTweets();

                if (LOGGER.isLoggable(Level.FINE)) {
                    LOGGER.fine(dataset + " " + partition + " " + gen.getNumFlushedTweets() + "\t"
                            + ((currentMeasureTweets - prevMeasuredTweets) / tputDuration) + " ID "
                            + Thread.currentThread().getId());
                }

                prevMeasuredTweets = currentMeasureTweets;

            }

            public void setPartition(int partition) {
                this.partition = partition;
            }
        }
    }

    @Override
    public void stop() throws Exception {
        twitterServer.stop();
    }

}
