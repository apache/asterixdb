package edu.uci.ics.asterix.metadata.feeds;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.CharBuffer;
import java.util.concurrent.LinkedBlockingQueue;

public class FeedMessageService {

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
                while (process) {
                    String message = inbox.take();
                    sfmSocket.getOutputStream().write(message.getBytes());
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println("STOPPING MESSAGE HANDLER");
                if (sfmSocket != null) {
                    try {
                        sfmSocket.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

        }

        public void stop() {
            process = false;
        }

        private Socket obtainSFMSocket() throws UnknownHostException, IOException, Exception {
            Socket sfmDirServiceSocket = null;
            FeedMessageService fsm = FeedManager.INSTANCE.getFeedMessageService(feedId);
            while (fsm == null) {
                fsm = FeedManager.INSTANCE.getFeedMessageService(feedId);
                if (fsm == null) {
                    Thread.sleep(2000);
                } else {
                    break;
                }
            }
            SuperFeedManager sfm = FeedManager.INSTANCE.getSuperFeedManager(feedId);

            System.out.println(" OBTAINED SFM DETAILS WILL TRY TO CONNECT " + sfm);

            try {
                sfmDirServiceSocket = new Socket(sfm.getHost(), sfm.getPort());
                System.out.println(" CONNECTED TO " + sfm.getHost() + " " + sfm.getPort());

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
                System.out.println("OBTAINED PORT " + port + " WILL CONNECT AT " + sfm.getHost() + " " + port);

                sfmSocket = new Socket(sfm.getHost(), port);
            } catch (Exception e) {
                System.out.println(" COULT NOT CONNECT TO " + sfm);
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
