/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.api.http.servlet;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.CharBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.feeds.FeedLifecycleListener;
import org.apache.asterix.metadata.feeds.RemoteSocketMessageListener;

public class FeedServletUtil {

    private static final Logger LOGGER = Logger.getLogger(FeedServletUtil.class.getName());
    private static final char EOL = (char) "\n".getBytes()[0];

    public static final class Constants {
        public static final String TABLE_HEADER_FEED_NAME = "Feed";
        public static final String TABLE_HEADER_DATASET_NAME = "Dataset";
        public static final String TABLE_HEADER_ACTIVE_SINCE = "Timestamp";
        public static final String TABLE_HEADER_INTAKE_STAGE = "Intake Stage";
        public static final String TABLE_HEADER_COMPUTE_STAGE = "Compute Stage";
        public static final String TABLE_HEADER_STORE_STAGE = "Store Stage";
        public static final String TABLE_HEADER_INTAKE_RATE = "Intake";
        public static final String TABLE_HEADER_STORE_RATE = "Store";
    }

    public static void initiateSubscription(FeedConnectionId feedId, String host, int port) throws IOException {
        LinkedBlockingQueue<String> outbox = new LinkedBlockingQueue<String>();
        int subscriptionPort = port + 1;
        Socket sc = new Socket(host, subscriptionPort);
        InputStream in = sc.getInputStream();

        CharBuffer buffer = CharBuffer.allocate(50);
        char ch = 0;
        while (ch != EOL) {
            buffer.put(ch);
            ch = (char) in.read();
        }
        buffer.flip();
        sc.close();

        String s = new String(buffer.array());
        int feedSubscriptionPort = Integer.parseInt(s.trim());
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Response from Super Feed Manager Report Service " + port + " will connect at " + host + " "
                    + port);
        }

        // register the feed subscription queue with FeedLifecycleListener
        FeedLifecycleListener.INSTANCE.registerFeedReportQueue(feedId, outbox);
        RemoteSocketMessageListener listener = new RemoteSocketMessageListener(host, feedSubscriptionPort, outbox);
        listener.start();
    }
}
