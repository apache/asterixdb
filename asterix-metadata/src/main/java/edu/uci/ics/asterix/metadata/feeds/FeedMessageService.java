package edu.uci.ics.asterix.metadata.feeds;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.CharBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sends feed report messages on behalf of an operator instance
 * to the SuperFeedMaanger associated with the feed.
 */
public class FeedMessageService {

    private static final Logger LOGGER = Logger.getLogger(FeedMessageService.class.getName());
    private static final char EOL = (char) "\n".getBytes()[0];

    private final FeedConnectionId feedId;
    private final LinkedBlockingQueue<String> inbox;
    private final FeedMessageHandler mesgHandler;

    public FeedMessageService(FeedConnectionId feedId) {
        this.feedId = feedId;
        this.inbox = new LinkedBlockingQueue<String>();
        mesgHandler = new FeedMessageHandler(inbox, feedId);
    }

    public void start() throws UnknownHostException, IOException, Exception {
        FeedManager.INSTANCE.getFeedExecutorService(feedId).execute(mesgHandler);
    }

    public void stop() throws IOException {
        mesgHandler.stop();
    }

    public void sendMessage(String message) throws IOException {
        inbox.add(message);
    }

    private static class FeedMessageHandler implements Runnable {

        private final LinkedBlockingQueue<String> inbox;
        private final FeedConnectionId feedId;
        private Socket sfmSocket;
        private boolean process = true;

        public FeedMessageHandler(LinkedBlockingQueue<String> inbox, FeedConnectionId feedId) {
            this.inbox = inbox;
            this.feedId = feedId;
        }

        public void run() {
            try {
                sfmSocket = obtainSFMSocket();
                if (sfmSocket != null) {
                    while (process) {
                        String message = inbox.take();
                        sfmSocket.getOutputStream().write(message.getBytes());
                    }
                } else {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Unable to start feed message service for " + feedId);
                    }
                }
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("ENDED FEED MESSAGE SERVICE for " + feedId);
                }
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Exception in handling incoming feed messages" + e.getMessage());
                }
            } finally {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Stopping feed message handler");
                }
                if (sfmSocket != null) {
                    try {
                        sfmSocket.close();
                    } catch (Exception e) {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Exception in closing socket " + e.getMessage());
                        }
                    }
                }
            }

        }

        public void stop() {
            process = false;
        }

        private Socket obtainSFMSocket() throws UnknownHostException, IOException, Exception {
            Socket sfmDirServiceSocket = null;
            SuperFeedManager sfm = FeedManager.INSTANCE.getSuperFeedManager(feedId);
            try {
                FeedRuntimeManager runtimeManager = FeedManager.INSTANCE.getFeedRuntimeManager(feedId);
                sfmDirServiceSocket = runtimeManager.createClientSocket(sfm.getHost(), sfm.getPort(),
                        FeedManager.SOCKET_CONNECT_TIMEOUT);
                if (sfmDirServiceSocket == null) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Unable to connect to " + sfm.getHost() + "[" + sfm.getPort() + "]");
                    }
                } else {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info(" Connected to Super Feed Manager service " + sfm.getHost() + " " + sfm.getPort());
                    }
                    while (!sfmDirServiceSocket.isConnected()) {
                        Thread.sleep(2000);
                    }
                    InputStream in = sfmDirServiceSocket.getInputStream();
                    CharBuffer buffer = CharBuffer.allocate(50);
                    char ch = 0;
                    while (ch != EOL) {
                        buffer.put(ch);
                        ch = (char) in.read();
                    }
                    buffer.flip();
                    String s = new String(buffer.array());
                    int port = Integer.parseInt(s.trim());
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Response from Super Feed Manager service " + port + " will connect at "
                                + sfm.getHost() + " " + port);
                    }
                    sfmSocket = runtimeManager.createClientSocket(sfm.getHost(), port,
                            FeedManager.SOCKET_CONNECT_TIMEOUT);
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            } finally {
                if (sfmDirServiceSocket != null) {
                    sfmDirServiceSocket.close();
                }
            }
            return sfmSocket;
        }
    }

}
